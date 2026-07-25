using System.Net;
using System.Threading;
using ROSettaDDS.Cdr;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Dds;
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rcl.Naming;
using ROSettaDDS.Rtps.Writer;
using ROSettaDDS.Transport;
using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Rcl;

/// <summary>
/// ROS 2 の rcl_context_t 相当。ドメイン共通の DDS 資源を所有する。
/// 1 プロセス内で複数 Node をホストできる。
/// </summary>
public sealed class Context : IDisposable
{
    private readonly ContextOptions _options;
    private readonly ParticipantTransportSet _transports;
    private readonly ParticipantRtpsReceiver _receiver;
    private readonly DiscoveryDb _discoveryDb;
    private readonly LeaseExpiryMonitor _leaseExpiryMonitor;
    private readonly SpdpBuiltinParticipantReader _spdpReader;
    private readonly SpdpBuiltinParticipantWriter _spdpWriter;
    private readonly SedpEndpointWriter _sedpPublicationsWriter;
    private readonly SedpEndpointReader _sedpPublicationsReader;
    private readonly SedpEndpointWriter _sedpSubscriptionsWriter;
    private readonly SedpEndpointReader _sedpSubscriptionsReader;
    private readonly NetworkRecoveryCoordinator? _networkRecovery;
    private readonly SemaphoreSlim _networkRecoveryGate = new(1, 1);
    private readonly UserEntityIdAllocator _userEntityIds = new();
    private readonly List<Node> _nodes = new();
    private readonly object _nodesLock = new();
    private readonly object _graphLock = new();

    private bool _started;
    private bool _disposed;

    public Context(ContextOptions options)
        : this(options, SystemNetworkChangeSource.Instance)
    {
    }

    internal Context(ContextOptions options, INetworkChangeSource networkChangeSource)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        if (networkChangeSource is null) throw new ArgumentNullException(nameof(networkChangeSource));
        _options = options;

        GuidPrefix = GuidPrefix.CreateForCurrentProcess(_options.VendorId);
        Guid = new Guid(GuidPrefix, BuiltinEntityIds.Participant);

        _transports = ParticipantTransportSet.Create(_options);
        _receiver = new ParticipantRtpsReceiver(GuidPrefix, _options.Logger);

        _discoveryDb = new DiscoveryDb(_options.DiscoveryLimits);
        _discoveryDb.ExternalLockEnter = () => Monitor.Enter(_graphLock);
        _discoveryDb.ExternalLockExit = () => Monitor.Exit(_graphLock);
        _leaseExpiryMonitor = new LeaseExpiryMonitor(_discoveryDb, _options, _options.Logger);

        _spdpReader = new SpdpBuiltinParticipantReader(
            _transports.MetatrafficMulticast, _discoveryDb, GuidPrefix, _options.Logger, limits: _options.DiscoveryLimits);

        _spdpWriter = new SpdpBuiltinParticipantWriter(
            transport: _transports.MetatrafficMulticast,
            multicastDestination: _transports.MetatrafficMulticastDestination,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: GuidPrefix,
            participantDataProvider: BuildParticipantData,
            interval: _options.SpdpInterval,
            logger: _options.Logger);

        _sedpPublicationsWriter = new SedpEndpointWriter(
            transport: _transports.MetatrafficMulticast,
            multicastDestination: _transports.MetatrafficMulticastDestination,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: GuidPrefix,
            writerEntityId: BuiltinEntityIds.SedpBuiltinPublicationsWriter,
            heartbeatPeriod: _options.SedpInterval,
            logger: _options.Logger);

        _sedpPublicationsReader = new SedpEndpointReader(
            replyTransport: _transports.MetatrafficUnicast,
            discoveryDb: _discoveryDb,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: GuidPrefix,
            readerEntityId: BuiltinEntityIds.SedpBuiltinPublicationsReader,
            ackNackFallbackDestination: _transports.MetatrafficMulticastDestination,
            producedEndpointKind: EndpointKind.Writer,
            logger: _options.Logger,
            limits: _options.DiscoveryLimits);

        _sedpSubscriptionsWriter = new SedpEndpointWriter(
            transport: _transports.MetatrafficMulticast,
            multicastDestination: _transports.MetatrafficMulticastDestination,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: GuidPrefix,
            writerEntityId: BuiltinEntityIds.SedpBuiltinSubscriptionsWriter,
            heartbeatPeriod: _options.SedpInterval,
            logger: _options.Logger);

        _sedpSubscriptionsReader = new SedpEndpointReader(
            replyTransport: _transports.MetatrafficUnicast,
            discoveryDb: _discoveryDb,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: GuidPrefix,
            readerEntityId: BuiltinEntityIds.SedpBuiltinSubscriptionsReader,
            ackNackFallbackDestination: _transports.MetatrafficMulticastDestination,
            producedEndpointKind: EndpointKind.Reader,
            logger: _options.Logger,
            limits: _options.DiscoveryLimits);

        // builtin endpoint を単一 receiver のルーティング対象として登録する。
        _receiver.RegisterReader(BuiltinEntityIds.SpdpBuiltinParticipantReader, _spdpReader);
        _receiver.RegisterReader(BuiltinEntityIds.SedpBuiltinPublicationsReader, _sedpPublicationsReader.Stateful);
        _receiver.RegisterReader(BuiltinEntityIds.SedpBuiltinSubscriptionsReader, _sedpSubscriptionsReader.Stateful);
        _receiver.RegisterWriter(BuiltinEntityIds.SedpBuiltinPublicationsWriter, _sedpPublicationsWriter.Stateful);
        _receiver.RegisterWriter(BuiltinEntityIds.SedpBuiltinSubscriptionsWriter, _sedpSubscriptionsWriter.Stateful);

        // SPDP で remote participant を発見/更新したら SEDP endpoint を auto-match
        _discoveryDb.ParticipantDiscovered += OnRemoteParticipantDiscovered;
        _discoveryDb.ParticipantUpdated += OnRemoteParticipantDiscovered;
        _discoveryDb.ParticipantLost += OnRemoteParticipantLost;

        if (_options.EnableAutomaticNetworkRecovery)
        {
            _networkRecovery = new NetworkRecoveryCoordinator(
                networkChangeSource,
                RecoverNetworkAsync,
                _options.Logger);
        }
    }

    public GuidPrefix GuidPrefix { get; }
    public Guid Guid { get; }
    public ContextOptions Options => _options;
    public ILogger Logger => _options.Logger;

    public int ResolvedParticipantId => _transports.ResolvedParticipantId;
    public IRtpsTransport UserMulticastTransport => _transports.UserMulticast;
    public IRtpsTransport UserUnicastTransport => _transports.UserUnicast;
    public Locator UserMulticastDestination => _transports.UserMulticastDestination;
    public DiscoveryDb DiscoveryDb => _discoveryDb;

    // ----- Node からの借用口 (internal) -----

    internal ParticipantTransportSet Transports => _transports;
    internal ParticipantRtpsReceiver Receiver => _receiver;
    internal bool IsDisposed => _disposed;
    internal CancellationToken LeaseExpiryCancellationToken => _leaseExpiryMonitor.CancellationToken;
    internal UserEntityIdAllocator UserEntityIds => _userEntityIds;
    internal object GraphLock => _graphLock;
    internal int PublishedPublicationStateCount => _sedpPublicationsWriter.PublishedCount;

    // テスト用 hook (null のままでは本番動作に影響しない)
    internal Action? GraphSnapshotEnterLockCallback { get; set; }
    internal Action? GraphSnapshotPauseCallback { get; set; }
    internal Action? GraphSnapshotBetweenLocalCollectionsCallback { get; set; }
    internal Action<object>? GraphLockMutationCallback { get; set; }

    /// <summary>テスト用: SEDP advertise (AddSubscriptionAsync / AddPublicationAsync) の直前に呼ばれる。</summary>
    internal Func<ValueTask>? SedpAdvertiseDelay { get; set; }

    /// <summary>テスト用: Node が使う IEndpointReceiver の代替。</summary>
    internal IEndpointReceiver? ReceiverOverrideForTest { get; set; }

    internal int PublishedSubscriptionStateCount => _sedpSubscriptionsWriter.PublishedCount;

    // ----- SEDP 広告の Node 向け delegate -----

    internal async ValueTask AddPublicationAsync(DiscoveredEndpointData endpointData, CancellationToken token)
    {
        if (SedpAdvertiseDelay is not null)
            await SedpAdvertiseDelay().ConfigureAwait(false);
        await _sedpPublicationsWriter.AddEndpointAsync(endpointData, token).ConfigureAwait(false);
    }

    internal async ValueTask AddSubscriptionAsync(DiscoveredEndpointData endpointData, CancellationToken token)
    {
        if (SedpAdvertiseDelay is not null)
            await SedpAdvertiseDelay().ConfigureAwait(false);
        await _sedpSubscriptionsWriter.AddEndpointAsync(endpointData, token).ConfigureAwait(false);
    }

    internal ValueTask UnregisterPublicationAsync(DiscoveredEndpointData endpoint)
        => _sedpPublicationsWriter.UnregisterEndpointAsync(endpoint);

    internal ValueTask UnregisterSubscriptionAsync(DiscoveredEndpointData endpoint)
        => _sedpSubscriptionsWriter.UnregisterEndpointAsync(endpoint);

    public void Start()
    {
        ThrowIfDisposed();
        if (_started) return;
        _transports.Start();

        // participant 単位の単一 receiver が全 transport の受信を 1 経路に集約する。
        _receiver.Subscribe(_transports.MetatrafficMulticast);
        _receiver.Subscribe(_transports.MetatrafficUnicast);
        _receiver.Subscribe(_transports.UserMulticast);
        _receiver.Subscribe(_transports.UserUnicast);

        _spdpWriter.Start();
        _sedpPublicationsWriter.Start();
        _sedpSubscriptionsWriter.Start();
        _leaseExpiryMonitor.Start();
        _started = true;
    }

    public void Stop()
    {
        ThrowIfDisposed();
        if (!_started) return;
        _leaseExpiryMonitor.Stop();
        _sedpPublicationsWriter.Stop();
        _sedpSubscriptionsWriter.Stop();
        _receiver.UnsubscribeAll();
        _spdpWriter.Stop();
        _transports.Stop();
        _started = false;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _networkRecovery?.Dispose();
        // Stop() は _disposed をチェックするので、先に Stop() してから _disposed = true にする。
        Stop();
        DisposeTrackedNodes();
        _disposed = true;
        _sedpPublicationsWriter.Dispose();
        _sedpSubscriptionsWriter.Dispose();
        _sedpPublicationsReader.Dispose();
        _sedpSubscriptionsReader.Dispose();
        _spdpWriter.Dispose();
        _spdpReader.Dispose();
        _leaseExpiryMonitor.Dispose();
        _receiver.Dispose();
        _transports.Dispose();
    }

    internal async ValueTask RecoverNetworkAsync(CancellationToken cancellationToken)
    {
        await _networkRecoveryGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        bool restartDiscoveryWriters = false;
        try
        {
            if (_disposed)
            {
                return;
            }

            restartDiscoveryWriters = _started;
            if (restartDiscoveryWriters)
            {
                _spdpWriter.Stop();
                _sedpPublicationsWriter.Stop();
                _sedpSubscriptionsWriter.Stop();
            }

            _transports.RestartOwnedTransports();
            var nodes = SnapshotNodes();
            foreach (var node in nodes)
            {
                EndpointDiscoverySnapshot endpoints;
                lock (_graphLock)
                {
                    endpoints = node.RefreshLocalEndpointLocators(
                        _transports.DefaultUnicastLocators,
                        _transports.UserMulticastDestination);
                }
                foreach (var writer in endpoints.Writers)
                {
                    await _sedpPublicationsWriter.AddEndpointAsync(writer, cancellationToken)
                        .ConfigureAwait(false);
                }
                foreach (var reader in endpoints.Readers)
                {
                    await _sedpSubscriptionsWriter.AddEndpointAsync(reader, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            _options.Logger.Info("Context: network transport recovery completed");
        }
        finally
        {
            if (restartDiscoveryWriters && !_disposed)
            {
                _sedpPublicationsWriter.Start();
                _sedpSubscriptionsWriter.Start();
                _spdpWriter.Start();
            }
            _networkRecoveryGate.Release();
        }
    }

    internal void RegisterNode(Node node)
    {
        ThrowIfDisposed();
        lock (_nodesLock) _nodes.Add(node);
    }

    internal void UnregisterNode(Node node)
    {
        lock (_nodesLock) _nodes.Remove(node);
    }

    private void DisposeTrackedNodes()
    {
        Node[] snapshot;
        lock (_nodesLock) snapshot = _nodes.ToArray();
        foreach (var node in snapshot)
        {
            try { node.Dispose(); }
            catch (Exception ex) { _options.Logger.Warn($"Context.Dispose failed to dispose Node: {ex.Message}", ex); }
        }
    }

    private Node[] SnapshotNodes()
    {
        lock (_nodesLock)
        {
            return _nodes.ToArray();
        }
    }

    /// <summary>
    /// Context graph lock 下で全 Node の local endpoint と DiscoveryDb の remote endpoint を
    /// 値コピーし、GUID 重複を除外して topic 名・GUID ordinal 順に並べたスナップショットを返す。
    /// local mutation 経路も同一 graph lock を取得するため、競合 stable な境界を提供する。
    /// </summary>
    internal GraphSnapshot CreateGraphSnapshot()
    {
        ThrowIfDisposed();
        var result = CollectSnapshotCore();
        return new GraphSnapshot(result.endpoints.AsReadOnly());
    }

    /// <summary>
    /// <see cref="CreateGraphSnapshot"/> と同じ snapshot に加え、local GUID 集合を同時に返す。
    /// diagnostics など local/remote 判定が必要な呼び出し元向け。
    /// </summary>
    internal (GraphSnapshot Snapshot, HashSet<Guid> LocalGuids) CreateGraphSnapshotWithLocalInfo()
    {
        ThrowIfDisposed();
        var result = CollectSnapshotCore();
        return (new GraphSnapshot(result.endpoints.AsReadOnly()), result.localGuids);
    }

    private (List<DiscoveredEndpointData> endpoints, HashSet<Guid> localGuids) CollectSnapshotCore()
    {
        // _nodesLock → _graphLock の順で取得し、RegisterNode/UnregisterNode と逆転しない。
        lock (_nodesLock)
        lock (_graphLock)
        {
            GraphSnapshotEnterLockCallback?.Invoke();
            GraphSnapshotPauseCallback?.Invoke();
            if (_disposed) return (new List<DiscoveredEndpointData>(), new HashSet<Guid>());

            var localWriters = new List<DiscoveredEndpointData>();
            var localReaders = new List<DiscoveredEndpointData>();

            foreach (var node in _nodes)
            {
                var local = node.LocalEndpointSnapshot();
                localWriters.AddRange(local.Writers);
                localReaders.AddRange(local.Readers);
            }

            var remote = _discoveryDb.CreateEndpointSnapshot();

            var seen = new HashSet<Guid>();
            var all = new List<DiscoveredEndpointData>();

            foreach (var w in localWriters) { if (seen.Add(w.EndpointGuid)) all.Add(w); }
            foreach (var w in remote.Writers) { if (seen.Add(w.EndpointGuid)) all.Add(w); }
            foreach (var r in localReaders) { if (seen.Add(r.EndpointGuid)) all.Add(r); }
            foreach (var r in remote.Readers) { if (seen.Add(r.EndpointGuid)) all.Add(r); }

            all.Sort(static (a, b) =>
            {
                int topicCmp = string.CompareOrdinal(a.TopicName, b.TopicName);
                if (topicCmp != 0) return topicCmp;
                return CompareGuid(a.EndpointGuid, b.EndpointGuid);
            });

            // 全 endpoint metadata 収集完了 → local GUID 収集直前
            GraphSnapshotBetweenLocalCollectionsCallback?.Invoke();

            var localGuids = new HashSet<Guid>();
            foreach (var w in localWriters) localGuids.Add(w.EndpointGuid);
            foreach (var r in localReaders) localGuids.Add(r.EndpointGuid);

            return (all, localGuids);
        }
    }

    private static int CompareGuid(Guid a, Guid b)
    {
        Span<byte> aBytes = stackalloc byte[GuidPrefix.Size];
        Span<byte> bBytes = stackalloc byte[GuidPrefix.Size];
        a.Prefix.CopyTo(aBytes);
        b.Prefix.CopyTo(bBytes);
        int cmp = aBytes.SequenceCompareTo(bBytes);
        return cmp != 0 ? cmp : a.EntityId.Value.CompareTo(b.EntityId.Value);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(GetType().Name);
    }

    private void OnRemoteParticipantDiscovered(RemoteParticipant participant)
    {
        // remote SEDP endpoint の Guid を計算 (固定 EntityId)
        var prefix = participant.GuidPrefix;
        var remoteSedpPubReader = new Guid(prefix, BuiltinEntityIds.SedpBuiltinPublicationsReader);
        var remoteSedpPubWriter = new Guid(prefix, BuiltinEntityIds.SedpBuiltinPublicationsWriter);
        var remoteSedpSubReader = new Guid(prefix, BuiltinEntityIds.SedpBuiltinSubscriptionsReader);
        var remoteSedpSubWriter = new Guid(prefix, BuiltinEntityIds.SedpBuiltinSubscriptionsWriter);

        // remote の metatraffic unicast (ACKNACK 返送先 / DATA 送信先)
        Locator? remoteUnicast = participant.Data.MetatrafficUnicastLocators.Count > 0
            ? participant.Data.MetatrafficUnicastLocators[0]
            : null;

        // 自 writer ↔ remote reader
        _sedpPublicationsWriter.MatchRemoteReader(remoteSedpPubReader, remoteUnicast);
        _sedpSubscriptionsWriter.MatchRemoteReader(remoteSedpSubReader, remoteUnicast);

        // 自 reader ↔ remote writer (ACKNACK 返送先として remoteUnicast)
        _sedpPublicationsReader.MatchRemoteWriter(remoteSedpPubWriter, remoteUnicast);
        _sedpSubscriptionsReader.MatchRemoteWriter(remoteSedpSubWriter, remoteUnicast);

        _options.Logger.Debug($"Context: auto-matched SEDP endpoints for {participant.Guid}");
    }

    private void OnRemoteParticipantLost(RemoteParticipant participant)
    {
        var prefix = participant.GuidPrefix;
        _sedpPublicationsWriter.UnmatchRemoteReader(new Guid(prefix, BuiltinEntityIds.SedpBuiltinPublicationsReader));
        _sedpSubscriptionsWriter.UnmatchRemoteReader(new Guid(prefix, BuiltinEntityIds.SedpBuiltinSubscriptionsReader));
        _sedpPublicationsReader.UnmatchRemoteWriter(new Guid(prefix, BuiltinEntityIds.SedpBuiltinPublicationsWriter));
        _sedpSubscriptionsReader.UnmatchRemoteWriter(new Guid(prefix, BuiltinEntityIds.SedpBuiltinSubscriptionsWriter));

        _options.Logger.Debug($"Context: unmatched SEDP endpoints for lost participant {participant.Guid}");
    }

    internal ParticipantData BuildParticipantData()
    {
        var data = new ParticipantData
        {
            ProtocolVersion = _options.ProtocolVersion,
            VendorId = _options.VendorId,
            Guid = Guid,
            BuiltinEndpoints = BuiltinEndpointSet.ROSettaDDSDefault,
            LeaseDuration = _options.LeaseDuration,
            EntityName = _options.EntityName,
        };
        data.MetatrafficMulticastLocators.Add(_transports.MetatrafficMulticastDestination);
        data.MetatrafficUnicastLocators.AddRange(_transports.MetatrafficUnicastLocators);
        data.DefaultUnicastLocators.AddRange(_transports.DefaultUnicastLocators);
        data.DefaultMulticastLocators.Add(_transports.UserMulticastDestination);
        return data;
    }
}
