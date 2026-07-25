using ROSettaDDS.Common;
using ROSettaDDS.Dds;
using System.Collections.Concurrent;
using System.Text;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Discovery;

/// <summary>
/// 検出した remote Participant と endpoint を保持するスレッドセーフな DB。
/// SPDP 受信側から <see cref="UpsertParticipant"/>、SEDP 受信側から <see cref="UpsertEndpoint"/>。
/// </summary>
public sealed class DiscoveryDb
{
    private readonly object _lock = new();
    private readonly Dictionary<GuidPrefix, RemoteParticipant> _participants = new();
    private readonly Dictionary<Guid, RemoteEndpoint> _writers = new();
    private readonly Dictionary<Guid, RemoteEndpoint> _readers = new();
    private readonly DiscoveryLimits _limits;
    private readonly ConcurrentQueue<Action> _eventQueue = new();
    private int _draining;

    public DiscoveryDb(DiscoveryLimits? limits = null)
    {
        _limits = limits ?? DiscoveryLimits.Default;
    }

    /// <summary>
    /// Context から注入される graph lock の Enter/Exit。
    /// null のままでも DiscoveryDb 単体の動作に影響しない。
    /// </summary>
    internal Action? ExternalLockEnter { get; set; }
    internal Action? ExternalLockExit { get; set; }

    /// <summary>テスト用: CreateEndpointSnapshot が内部 _lock を取得した直後に呼ばれる。</summary>
    internal Action? SnapshotLockAcquiredCallback { get; set; }

    /// <summary>テスト用: UpsertEndpoint/TryRemoveEndpoint が内部 _lock を取得した直後に呼ばれる。</summary>
    internal Action? MutationLockAcquiredCallback { get; set; }

    /// <summary>新規参加者検出時に発火 (lock 外で呼ばれる)。</summary>
    public event Action<RemoteParticipant>? ParticipantDiscovered;

    /// <summary>既存参加者の更新時に発火 (lock 外で呼ばれる)。</summary>
    public event Action<RemoteParticipant>? ParticipantUpdated;

    /// <summary>Lease 期限切れで削除されたときに発火 (lock 外で呼ばれる)。</summary>
    public event Action<RemoteParticipant>? ParticipantLost;

    /// <summary>新規 remote Writer 検出時。</summary>
    public event Action<RemoteEndpoint>? WriterDiscovered;

    /// <summary>新規 remote Reader 検出時。</summary>
    public event Action<RemoteEndpoint>? ReaderDiscovered;

    /// <summary>endpoint が更新されたとき (Writer/Reader 共通)。</summary>
    public event Action<RemoteEndpoint>? EndpointUpdated;

    /// <summary>remote Writer が unregister/dispose されたとき。</summary>
    public event Action<RemoteEndpoint>? WriterLost;

    /// <summary>remote Reader が unregister/dispose されたとき。</summary>
    public event Action<RemoteEndpoint>? ReaderLost;

    /// <summary>現在登録されている Participant 数。</summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _participants.Count;
            }
        }
    }

    /// <summary>登録されている Participant のスナップショット。</summary>
    public IReadOnlyList<RemoteParticipant> Snapshot()
    {
        lock (_lock)
        {
            return _participants.Values.ToArray();
        }
    }

    /// <summary>
    /// SPDP 受信時に呼ぶ。新規なら追加して <see cref="ParticipantDiscovered"/>、
    /// 既存なら状態を更新して <see cref="ParticipantUpdated"/> を発火する。
    /// 自分自身 (Guid Prefix が <paramref name="ignorePrefix"/> と一致) は無視する。
    /// </summary>
    public void UpsertParticipant(ParticipantData data, DateTime nowUtc, GuidPrefix? ignorePrefix = null)
    {
        if (ignorePrefix.HasValue && data.Guid.Prefix.Equals(ignorePrefix.Value))
        {
            return;
        }
        if (!IsParticipantMetadataAccepted(data))
        {
            return;
        }
        data.LeaseDuration = _limits.ClampRemoteParticipantLeaseDuration(data.LeaseDuration);

        RemoteParticipant participant;
        bool isNew;
        ExternalLockEnter?.Invoke();
        try
        {
            lock (_lock)
            {
                if (_participants.TryGetValue(data.Guid.Prefix, out var existing))
                {
                    existing.Update(data, nowUtc);
                    participant = existing;
                    isNew = false;
                }
                else
                {
                    if (_participants.Count >= _limits.MaxRemoteParticipants)
                    {
                        return;
                    }
                    participant = new RemoteParticipant(data, nowUtc);
                    _participants[data.Guid.Prefix] = participant;
                    isNew = true;
                }

                EnqueueEvent(isNew
                    ? () => InvokeSafely(ParticipantDiscovered, participant)
                    : () => InvokeSafely(ParticipantUpdated, participant));
            }
        }
        finally
        {
            ExternalLockExit?.Invoke();
        }

        TryDispatch();
    }

    /// <summary>Lease 期限切れの Participant を削除し、それぞれ Lost イベントを発火する。</summary>
    public void ExpireOldParticipants(DateTime nowUtc)
    {
        List<RemoteParticipant>? expired = null;
        List<RemoteEndpoint>? lostWriters = null;
        List<RemoteEndpoint>? lostReaders = null;
        ExternalLockEnter?.Invoke();
        try
        {
            lock (_lock)
            {
                foreach (var (key, p) in _participants.ToArray())
                {
                    if (p.IsExpired(nowUtc))
                    {
                        expired ??= new List<RemoteParticipant>();
                        expired.Add(p);
                        _participants.Remove(key);
                        RemoveEndpointsForParticipant(key, ref lostWriters, ref lostReaders);
                    }
                }

                EnqueueLostEndpoints(lostWriters, lostReaders);
                if (expired is not null)
                {
                    foreach (var p in expired)
                    {
                        EnqueueEvent(() => InvokeSafely(ParticipantLost, p));
                    }
                }
            }
        }
        finally
        {
            ExternalLockExit?.Invoke();
        }

        TryDispatch();
    }

    /// <summary>明示的に Participant を削除する (テスト/シャットダウン用)。</summary>
    public bool TryRemove(GuidPrefix prefix)
    {
        RemoteParticipant? removed;
        List<RemoteEndpoint>? lostWriters = null;
        List<RemoteEndpoint>? lostReaders = null;
        ExternalLockEnter?.Invoke();
        try
        {
            lock (_lock)
            {
                if (!_participants.Remove(prefix, out removed))
                {
                    return false;
                }
                RemoveEndpointsForParticipant(prefix, ref lostWriters, ref lostReaders);

                EnqueueLostEndpoints(lostWriters, lostReaders);
                if (removed is not null)
                {
                    EnqueueEvent(() => InvokeSafely(ParticipantLost, removed));
                }
            }
        }
        finally
        {
            ExternalLockExit?.Invoke();
        }

        TryDispatch();
        return true;
    }

    /// <summary>
    /// SEDP 受信時に呼ぶ。新規なら追加して Discovered イベント、既存なら Update で EndpointUpdated。
    /// 自分自身の Participant prefix と一致するものは無視する。
    /// </summary>
    public void UpsertEndpoint(DiscoveredEndpointData data, DateTime nowUtc, GuidPrefix? ignorePrefix = null)
    {
        if (ignorePrefix.HasValue && data.EndpointGuid.Prefix.Equals(ignorePrefix.Value))
        {
            return;
        }
        if (!IsEndpointMetadataAccepted(data))
        {
            return;
        }

        var dict = data.Kind == EndpointKind.Writer ? _writers : _readers;
        RemoteEndpoint endpoint;
        bool isNew;
        ExternalLockEnter?.Invoke();
        try
        {
            lock (_lock)
            {
                MutationLockAcquiredCallback?.Invoke();
                if (!_participants.ContainsKey(data.ParticipantGuid.Prefix)
                    || !data.EndpointGuid.Prefix.Equals(data.ParticipantGuid.Prefix))
                {
                    return;
                }
                if (dict.TryGetValue(data.EndpointGuid, out var existing))
                {
                    existing.Update(data, nowUtc);
                    endpoint = existing;
                    isNew = false;
                }
                else
                {
                    if (IsEndpointCapacityExceeded(data.Kind)
                        || CountEndpointsForParticipant(data.ParticipantGuid.Prefix) >= _limits.MaxRemoteEndpointsPerParticipant)
                    {
                        return;
                    }
                    endpoint = new RemoteEndpoint(data, nowUtc);
                    dict[data.EndpointGuid] = endpoint;
                    isNew = true;
                }

                if (isNew)
                {
                    EnqueueEvent(data.Kind == EndpointKind.Writer
                        ? () => InvokeSafely(WriterDiscovered, endpoint)
                        : () => InvokeSafely(ReaderDiscovered, endpoint));
                }
                else
                {
                    EnqueueEvent(() => InvokeSafely(EndpointUpdated, endpoint));
                }
            }
        }
        finally
        {
            ExternalLockExit?.Invoke();
        }

        TryDispatch();
    }

    public bool TryRemoveEndpoint(EndpointKind kind, Guid endpointGuid, GuidPrefix? ignorePrefix = null)
    {
        if (ignorePrefix.HasValue && endpointGuid.Prefix.Equals(ignorePrefix.Value))
        {
            return false;
        }

        var dict = kind == EndpointKind.Writer ? _writers : _readers;
        RemoteEndpoint? removed;
        ExternalLockEnter?.Invoke();
        try
        {
            lock (_lock)
            {
                MutationLockAcquiredCallback?.Invoke();
                if (!dict.Remove(endpointGuid, out removed))
                {
                    return false;
                }

                if (removed is not null)
                {
                    EnqueueEvent(kind == EndpointKind.Writer
                        ? () => InvokeSafely(WriterLost, removed)
                        : () => InvokeSafely(ReaderLost, removed));
                }
            }
        }
        finally
        {
            ExternalLockExit?.Invoke();
        }

        TryDispatch();
        return true;
    }

    /// <summary>登録されている Writer 数。</summary>
    public int WriterCount
    {
        get { lock (_lock) { return _writers.Count; } }
    }

    /// <summary>登録されている Reader 数。</summary>
    public int ReaderCount
    {
        get { lock (_lock) { return _readers.Count; } }
    }

    /// <summary>登録されている Writer のスナップショット。</summary>
    public IReadOnlyList<RemoteEndpoint> WriterSnapshot()
    {
        lock (_lock) { return _writers.Values.ToArray(); }
    }

    /// <summary>登録されている Reader のスナップショット。</summary>
    public IReadOnlyList<RemoteEndpoint> ReaderSnapshot()
    {
        lock (_lock) { return _readers.Values.ToArray(); }
    }

    /// <summary>
    /// Writer/Reader のメタデータを同じロック区間で値コピーしたスナップショットを返す。
    /// 取得後の DiscoveredEndpointData を変更しても内部状態に影響しない。
    /// </summary>
    internal EndpointDiscoverySnapshot CreateEndpointSnapshot()
    {
        lock (_lock)
        {
            SnapshotLockAcquiredCallback?.Invoke();
            var writers = _writers.Values
                .Select(static w => w.Data.Clone())
                .ToArray();
            var readers = _readers.Values
                .Select(static r => r.Data.Clone())
                .ToArray();
            return new EndpointDiscoverySnapshot(writers, readers);
        }
    }

    private void RemoveEndpointsForParticipant(
        GuidPrefix participantPrefix,
        ref List<RemoteEndpoint>? lostWriters,
        ref List<RemoteEndpoint>? lostReaders)
    {
        RemoveEndpointsForParticipant(_writers, participantPrefix, ref lostWriters);
        RemoveEndpointsForParticipant(_readers, participantPrefix, ref lostReaders);
    }

    private static void RemoveEndpointsForParticipant(
        Dictionary<Guid, RemoteEndpoint> endpoints,
        GuidPrefix participantPrefix,
        ref List<RemoteEndpoint>? lostEndpoints)
    {
        foreach (var (endpointGuid, endpoint) in endpoints.ToArray())
        {
            if (!BelongsToParticipant(endpoint, participantPrefix))
            {
                continue;
            }
            endpoints.Remove(endpointGuid);
            lostEndpoints ??= new List<RemoteEndpoint>();
            lostEndpoints.Add(endpoint);
        }
    }

    private static bool BelongsToParticipant(RemoteEndpoint endpoint, GuidPrefix participantPrefix)
        => endpoint.Guid.Prefix.Equals(participantPrefix)
        || endpoint.ParticipantGuid.Prefix.Equals(participantPrefix);

    private bool IsEndpointCapacityExceeded(EndpointKind kind)
        => kind == EndpointKind.Writer
            ? _writers.Count >= _limits.MaxRemoteWriters
            : _readers.Count >= _limits.MaxRemoteReaders;

    private int CountEndpointsForParticipant(GuidPrefix participantPrefix)
    {
        int count = 0;
        foreach (var endpoint in _writers.Values)
        {
            if (BelongsToParticipant(endpoint, participantPrefix))
            {
                count++;
            }
        }
        foreach (var endpoint in _readers.Values)
        {
            if (BelongsToParticipant(endpoint, participantPrefix))
            {
                count++;
            }
        }
        return count;
    }

    private bool IsParticipantMetadataAccepted(ParticipantData data)
    {
        int locatorCount = data.MetatrafficUnicastLocators.Count
            + data.MetatrafficMulticastLocators.Count
            + data.DefaultUnicastLocators.Count
            + data.DefaultMulticastLocators.Count;
        if (locatorCount > _limits.MaxParticipantLocators)
        {
            return false;
        }
        return StringByteCount(data.EntityName) <= _limits.MaxEntityNameBytes;
    }

    private void EnqueueEvent(Action fire)
    {
        _eventQueue.Enqueue(fire);
    }

    private void TryDispatch()
    {
        while (true)
        {
            Action? action;
            lock (_lock)
            {
                if (_draining != 0)
                    return;
                _draining = 1;
                if (!_eventQueue.TryDequeue(out action))
                {
                    _draining = 0;
                    return;
                }
            }

            do
            {
                if (action is not null)
                {
                    try { action(); } catch { }
                }

                lock (_lock)
                {
                    if (_eventQueue.TryDequeue(out action))
                        continue;
                    _draining = 0;
                    return;
                }
            } while (true);
        }
    }

    private static void InvokeSafely<T>(Action<T>? handler, T arg)
    {
        if (handler is null)
            return;
        foreach (Delegate del in handler.GetInvocationList())
        {
            try
            {
                ((Action<T>)del)(arg);
            }
            catch
            {
            }
        }
    }

    private bool IsEndpointMetadataAccepted(DiscoveredEndpointData data)
    {
        int locatorCount = data.UnicastLocators.Count + data.MulticastLocators.Count;
        return locatorCount <= _limits.MaxEndpointLocators
            && StringByteCount(data.TopicName) <= _limits.MaxTopicNameBytes
            && StringByteCount(data.TypeName) <= _limits.MaxTypeNameBytes
            && data.Partition.Names.Count <= _limits.MaxPartitionNames
            && data.Partition.Names.All(name => StringByteCount(name) <= _limits.MaxPartitionNameBytes);
    }

    private static int StringByteCount(string? value)
        => string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetByteCount(value);

    private void EnqueueLostEndpoints(
        IReadOnlyList<RemoteEndpoint>? lostWriters,
        IReadOnlyList<RemoteEndpoint>? lostReaders)
    {
        if (lostWriters is not null)
        {
            foreach (var writer in lostWriters)
            {
                EnqueueEvent(() => InvokeSafely(WriterLost, writer));
            }
        }
        if (lostReaders is not null)
        {
            foreach (var reader in lostReaders)
            {
                EnqueueEvent(() => InvokeSafely(ReaderLost, reader));
            }
        }
    }
}
