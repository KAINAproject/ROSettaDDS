using ROSettaDDS.Cdr;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

/// <summary>
/// 型付き Subscription。<see cref="IUserReader"/> から SerializedPayload を受け取って
/// <see cref="ICdrSerializer{T}"/> でデシリアライズし、ハンドラへ渡す。
/// Best-Effort / Reliable のどちらの reader でも同じ経路で扱う。
/// </summary>
public sealed class Subscription<T> : IDisposable
{
    private readonly IUserReader _reader;
    private readonly ICdrSerializer<T> _serializer;
    private readonly Action<T, GuidPrefix> _handler;
    private readonly Action<Guid, IUserReader>? _unregisterEndpoint;
    private readonly SynchronizationContext? _handlerContext;
    private readonly ILogger _logger;
    private readonly CdrReadLimits _cdrReadLimits;
    internal Action? BeforeUnregister { get; set; }
    internal Action? RemoveFromTracker { get; set; }
    private long _payloadsReceivedFromReader;
    private long _messagesDeserialized;
    private long _deserializeFailures;
    private long _handlerInvocations;
    private int _disposed;
    private Task? _advertiseTask;
    private readonly ManualResetEventSlim _disposeCompleted = new();

    public string TopicName { get; }
    public Guid Guid { get; }
    public EntityId ReaderEntityId => _reader.ReaderEntityId;

    public SubscriptionDiagnostics Diagnostics => new(
        _reader.Diagnostics,
        Volatile.Read(ref _payloadsReceivedFromReader),
        Volatile.Read(ref _messagesDeserialized),
        Volatile.Read(ref _deserializeFailures),
        Volatile.Read(ref _handlerInvocations));

    /// <summary>Subscription マッチ状態 (Fast DDS 互換)。</summary>
    public SubscriptionMatchedStatus SubscriptionMatchedStatus => _reader.SubscriptionMatchedStatus;

    internal Subscription(
        string topicName,
        Guid guid,
        IUserReader reader,
        ICdrSerializer<T> serializer,
        Action<T, GuidPrefix> handler,
        Action<Guid, IUserReader>? unregisterEndpoint = null,
        SynchronizationContext? handlerContext = null,
        ILogger? logger = null,
        bool autoStart = true,
        CdrReadLimits? cdrReadLimits = null)
    {
        TopicName = topicName;
        Guid = guid;
        _reader = reader;
        _serializer = serializer;
        _handler = handler;
        _unregisterEndpoint = unregisterEndpoint;
        _handlerContext = handlerContext;
        _logger = logger ?? NullLogger.Instance;
        _cdrReadLimits = cdrReadLimits ?? CdrReadLimits.Default;
        _reader.PayloadReceived += OnPayloadReceived;
        if (autoStart)
        {
            _reader.Start();
        }
    }

    internal void SetAdvertiseTask(Task task) => _advertiseTask = task;

    private void OnPayloadReceived(ReadOnlyMemory<byte> payload, GuidPrefix sourcePrefix)
    {
        Interlocked.Increment(ref _payloadsReceivedFromReader);
        T value;
        try
        {
            value = DeserializeWithEncapsulation(payload.Span);
            Interlocked.Increment(ref _messagesDeserialized);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _deserializeFailures);
            _logger.Warn($"Subscription failed to deserialize payload on topic {TopicName}", ex);
            return;
        }

        if (_handlerContext is null)
        {
            InvokeHandler(value, sourcePrefix);
            return;
        }

        _handlerContext.Post(
            static state =>
            {
                var callback = (HandlerCallback)state!;
                callback.Subscription.InvokeHandler(callback.Value, callback.SourcePrefix);
            },
            new HandlerCallback(this, value, sourcePrefix));
    }

    private void InvokeHandler(T value, GuidPrefix sourcePrefix)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        try
        {
            _handler(value, sourcePrefix);
            Interlocked.Increment(ref _handlerInvocations);
        }
        catch (Exception ex)
        {
            _logger.Error($"Subscription handler failed on topic {TopicName}", ex);
        }
    }

    /// <summary>
    /// matched writer 数が <paramref name="minCount"/> に達するまで待機する。
    /// 戻り値: true=達成 / false=タイムアウト。
    /// </summary>
    public Task<bool> WaitForMatchedAsync(
        int minCount, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return MatchWaiter.WaitUntilMatchedAsync(
            () => _reader.MatchedWriterCount,
            minCount, timeout, cancellationToken);
    }

    /// <summary>encap header を解釈してデシリアライズする (テスト/デバッグ用)。</summary>
    public T DeserializeWithEncapsulation(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < CdrEncapsulation.Size)
        {
            throw new InvalidDataException(
                $"Payload too small for CDR encapsulation header (got {payload.Length} bytes).");
        }
        var (kind, _) = CdrEncapsulation.Read(payload[..CdrEncapsulation.Size]);
        var endian = CdrEncapsulation.GetEndianness(kind);
        var r = new CdrReader(payload, endian, cdrOrigin: CdrEncapsulation.Size, limits: _cdrReadLimits);
        _serializer.Deserialize(ref r, out var value);
        return value;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            _disposeCompleted.Wait();
            return;
        }

        try
        {
            _reader.PayloadReceived -= OnPayloadReceived;

            if (_advertiseTask is not null)
            {
                try { _advertiseTask.ConfigureAwait(false).GetAwaiter().GetResult(); }
                catch { }
            }

            _reader.Stop();
            BeforeUnregister?.Invoke();
            _unregisterEndpoint?.Invoke(Guid, _reader);
            _reader.Dispose();
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

    private sealed class HandlerCallback
    {
        public HandlerCallback(Subscription<T> subscription, T value, GuidPrefix sourcePrefix)
        {
            Subscription = subscription;
            Value = value;
            SourcePrefix = sourcePrefix;
        }

        public Subscription<T> Subscription { get; }
        public T Value { get; }
        public GuidPrefix SourcePrefix { get; }
    }
}
