using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rtps.Reader;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

/// <summary>
/// Best-Effort 用のユーザー Reader。<see cref="StatelessReader"/> をラップする。
/// ACKNACK を返さないため <c>unicastReplyLocator</c> は無視する。
/// </summary>
internal sealed class BestEffortUserReader : IUserReader
{
    private readonly StatelessReader _reader;
    private int _disposed;

    public BestEffortUserReader(
        GuidPrefix localPrefix,
        EntityId readerEntityId,
        ILogger? logger = null,
        DataFragReassemblyOptions? dataFragOptions = null)
    {
        _reader = new StatelessReader(localPrefix, readerEntityId, logger, dataFragOptions);
        Guid = new Guid(localPrefix, readerEntityId);
        _reader.PayloadReceived += OnPayloadReceived;
    }

    public EntityId ReaderEntityId => _reader.ReaderEntityId;
    public Guid Guid { get; }
    public IRtpsSubmessageHandler Handler => _reader;

    public event Action<ReadOnlyMemory<byte>, GuidPrefix>? PayloadReceived;

    public int MatchedWriterCount => _reader.MatchedWriterCount;

    public SubscriptionMatchedStatus SubscriptionMatchedStatus => _reader.SubscriptionMatchedStatus;

    public RtpsReaderDiagnostics Diagnostics => _reader.Diagnostics;

    public void MatchWriter(Guid writerGuid, Locator? unicastReplyLocator) => _reader.MatchWriter(writerGuid);
    public void UnmatchWriter(Guid writerGuid) => _reader.UnmatchWriter(writerGuid);

    public void Start() => _reader.Start();
    public void Stop() => _reader.Stop();

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _reader.PayloadReceived -= OnPayloadReceived;
        _reader.Dispose();
    }

    private void OnPayloadReceived(ReadOnlyMemory<byte> payload, GuidPrefix sourcePrefix)
        => PayloadReceived?.Invoke(payload, sourcePrefix);
}
