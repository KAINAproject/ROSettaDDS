using System.Buffers;
using System.Collections.Generic;
using ROSettaDDS.Cdr;
using ROSettaDDS.Common;
using ROSettaDDS.Rtps.HistoryCache;
using ROSettaDDS.Rtps.Writer;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

/// <summary>
/// 型付き Publisher。<see cref="ICdrSerializer{T}"/> でシリアライズして
/// <see cref="StatefulWriter"/> 経由で RELIABLE 配信する。
/// </summary>
public sealed class Publisher<T> : IDisposable
{
    private readonly StatefulWriter _writer;
    private readonly ICdrSerializer<T> _serializer;
    private readonly Action<Guid, StatefulWriter>? _unregisterEndpoint;
    internal Action? BeforeUnregister { get; set; }
    internal Action? RemoveFromTracker { get; set; }
    private int _disposed;
    private Task? _advertiseTask;
    private readonly ManualResetEventSlim _disposeCompleted = new();

    public string TopicName { get; }
    public Guid Guid => _writer.Guid;
    internal StatefulWriter Writer => _writer;

    /// <summary>Publication マッチ状態 (Fast DDS 互換)。</summary>
    public PublicationMatchedStatus PublicationMatchedStatus => _writer.PublicationMatchedStatus;

    public Publisher(
        string topicName,
        StatefulWriter writer,
        ICdrSerializer<T> serializer,
        Action<Guid, StatefulWriter>? unregisterEndpoint = null)
    {
        TopicName = topicName;
        _writer = writer;
        _serializer = serializer;
        _unregisterEndpoint = unregisterEndpoint;
    }

    /// <summary>値をシリアライズ (encap header CDR_LE 付き) して 1 件送信する。</summary>
    public async ValueTask PublishAsync(T value, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var (owner, memory) = SerializeOwned(value);
        // WriteOwnedAsync 内で Add 失敗時のみ owner は release 済み。
        // ここでは catch しない (二重 dispose / Use-After-Return 防止)。
        await _writer.WriteOwnedAsync(owner, memory, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>値を送信し、採番された RTPS シーケンス番号を返す (サービス用)。</summary>
    public ValueTask<SequenceNumber> PublishReturningSequenceNumberAsync(
        T value, CancellationToken cancellationToken = default)
        => PublishReturningSequenceNumberAsync(value, onSequenceAssigned: null, cancellationToken);

    /// <summary>値を送信し、SN 採番直後・送信前に <paramref name="onSequenceAssigned"/> を呼ぶ (サービス用)。</summary>
    public async ValueTask<SequenceNumber> PublishReturningSequenceNumberAsync(
        T value, Action<SequenceNumber>? onSequenceAssigned, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var payload = SerializeWithEncapsulation(value);
        return await _writer.WriteReturningSequenceNumberAsync(payload, onSequenceAssigned, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 複数の値を送信する。1 件ずつ rent → add → send のストリーミングで処理し、
    /// payload が大きくてもバッファを N 件同時保持しない。
    /// </summary>
    public async ValueTask PublishManyAsync(IReadOnlyList<T> values, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (values is null) throw new ArgumentNullException(nameof(values));
        if (values.Count == 0) return;

        int n = values.Count;
        for (int i = 0; i < n; i++)
        {
            // WriteOwnedAsync が history.Add 失敗時のみ owner を release する。
            // 成功時は所有権が history に移転し、evict / ACK / Dispose で解放される。
            var (owner, memory) = SerializeOwned(values[i]);
            await _writer.WriteOwnedAsync(owner, memory, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 同じ値を <paramref name="count"/> 回連続送信する shortcut。
    /// ストリーミング実装で 1 件ずつ rent → add → send するため、
    /// payload 8 KB × 200 件のような大きい batch でも
    /// ArrayPool バッファを N 件同時保持しない。
    /// </summary>
    public ValueTask PublishRepeatedAsync(T value, int count, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (count <= 0) return default;

        return PublishRepeatedCoreAsync(value, count, cancellationToken);
    }

    private async ValueTask PublishRepeatedCoreAsync(T value, int count, CancellationToken cancellationToken)
    {
        for (int i = 0; i < count; i++)
        {
            var (owner, memory) = SerializeOwned(value);
            await _writer.WriteOwnedAsync(owner, memory, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>シリアライズ後のバイト列 (encap header 込み) を返す (テスト/デバッグ用)。</summary>
    public ReadOnlyMemory<byte> SerializeWithEncapsulation(T value)
    {
        int sizeEstimate = _serializer.GetSerializedSize(value);
        int totalCapacity = CdrEncapsulation.Size + sizeEstimate + 16;
        var buffer = new byte[totalCapacity];
        CdrEncapsulation.Write(buffer, CdrEncapsulation.CdrLittleEndian);
        var w = new CdrWriter(buffer, CdrEndianness.LittleEndian, cdrOrigin: CdrEncapsulation.Size);
        _serializer.Serialize(ref w, in value);
        int payloadLength = w.Position;
        return buffer.AsMemory(0, payloadLength);
    }

    /// <summary>
    /// ArrayPool から rent した buffer に serialize し、所有者情報 (RtpsPayloadOwner) と
    /// ペイロード (ReadOnlyMemory) を返す。所有者は history に移転し、
    /// history.Add 失敗時のみ呼び出し側で release する。
    /// </summary>
    private (RtpsPayloadOwner owner, ReadOnlyMemory<byte> memory) SerializeOwned(T value)
    {
        int sizeEstimate = _serializer.GetSerializedSize(value);
        int totalCapacity = CdrEncapsulation.Size + sizeEstimate + 16;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(totalCapacity);
        try
        {
            CdrEncapsulation.Write(buffer, CdrEncapsulation.CdrLittleEndian);
            var w = new CdrWriter(buffer, CdrEndianness.LittleEndian, cdrOrigin: CdrEncapsulation.Size);
            _serializer.Serialize(ref w, in value);
            int payloadLength = w.Position;
            var owner = new RtpsPayloadOwner(buffer);
            return (owner, buffer.AsMemory(0, payloadLength));
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// matched reader 数が <paramref name="minCount"/> に達するまで待機する。
    /// 戻り値: true=達成 / false=タイムアウト。
    /// </summary>
    public Task<bool> WaitForMatchedAsync(
        int minCount, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return MatchWaiter.WaitUntilMatchedAsync(
            () => _writer.MatchedReaderCount,
            minCount, timeout, cancellationToken);
    }

    internal void SetAdvertiseTask(Task task) => _advertiseTask = task;

    public void Start() => _writer.Start();
    public void Stop() => _writer.Stop();

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            _disposeCompleted.Wait();
            return;
        }

        try
        {
            if (_advertiseTask is not null)
            {
                try { _advertiseTask.ConfigureAwait(false).GetAwaiter().GetResult(); }
                catch { }
            }

            _writer.Stop();
            BeforeUnregister?.Invoke();
            _unregisterEndpoint?.Invoke(Guid, _writer);
            _writer.Dispose();
            RemoveFromTracker?.Invoke();
        }
        finally
        {
            _disposeCompleted.Set();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(GetType().Name);
    }
}
