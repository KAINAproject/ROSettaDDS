using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rtps.HistoryCache;
using ROSettaDDS.Rtps.Reader;
using ROSettaDDS.Transport;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

/// <summary>
/// Reliable 用のユーザー Reader。<see cref="StatefulReader"/> をラップする。
/// HEARTBEAT に対して ACKNACK を返し、欠損 sample の再送を要求する。
/// </summary>
internal sealed class ReliableUserReader : IUserReader
{
    private readonly StatefulReader _reader;
    private int _disposed;

    public ReliableUserReader(
        IRtpsTransport replyTransport,
        ProtocolVersion version,
        VendorId vendorId,
        GuidPrefix localPrefix,
        EntityId readerEntityId,
        Locator ackNackFallbackDestination,
        ILogger? logger = null,
        DataFragReassemblyOptions? dataFragOptions = null)
    {
        _reader = new StatefulReader(
            replyTransport: replyTransport,
            version: version,
            vendorId: vendorId,
            localPrefix: localPrefix,
            readerEntityId: readerEntityId,
            ackNackFallbackDestination: ackNackFallbackDestination,
            logger: logger,
            dataFragOptions: dataFragOptions);
        Guid = _reader.Guid;
        _reader.PayloadReceived += OnSampleReceived;
    }

    public EntityId ReaderEntityId => _reader.ReaderEntityId;
    public Guid Guid { get; }
    public IRtpsSubmessageHandler Handler => _reader;

    public event Action<ReadOnlyMemory<byte>, GuidPrefix>? PayloadReceived;

    /// <summary>inline QoS を含む CacheChange を必要とする利用者向け (サービス reply 等)。</summary>
    public event Action<CacheChange>? SampleReceived;

    public int MatchedWriterCount => _reader.MatchedWriterCount;

    public SubscriptionMatchedStatus SubscriptionMatchedStatus => _reader.SubscriptionMatchedStatus;

    public RtpsReaderDiagnostics Diagnostics => _reader.Diagnostics;

    public void MatchWriter(Guid writerGuid, Locator? unicastReplyLocator)
        => _reader.MatchWriter(writerGuid, unicastReplyLocator);
    public void UnmatchWriter(Guid writerGuid) => _reader.UnmatchWriter(writerGuid);

    // StatefulReader は受信を transport 購読に依存しない (receiver が駆動する) ため no-op。
    public void Start() { }
    public void Stop() { }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _reader.PayloadReceived -= OnSampleReceived;
        _reader.Dispose();
    }

    private void OnSampleReceived(CacheChange change)
    {
        PayloadReceived?.Invoke(change.SerializedPayload, change.WriterGuid.Prefix);
        SampleReceived?.Invoke(change);
    }
}
