using ROSettaDDS.Common;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

internal sealed class RawSubscription : IDisposable
{
    private readonly IUserReader _reader;
    private readonly Action<ReadOnlyMemory<byte>, GuidPrefix> _callback;
    private readonly Action<Guid, IUserReader>? _unregisterEndpoint;
    internal Action? BeforeUnregister { get; set; }
    internal Action? RemoveFromTracker { get; set; }
    private int _disposed;
    private Task? _advertiseTask;
    private readonly ManualResetEventSlim _disposeCompleted = new();

    public string TopicName { get; }
    public Guid Guid { get; }

    internal RawSubscription(
        string topicName,
        Guid guid,
        IUserReader reader,
        Action<ReadOnlyMemory<byte>, GuidPrefix> callback,
        Action<Guid, IUserReader>? unregisterEndpoint = null,
        bool autoStart = true)
    {
        TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
        Guid = guid;
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _callback = callback ?? throw new ArgumentNullException(nameof(callback));
        _unregisterEndpoint = unregisterEndpoint;
        _reader.PayloadReceived += OnPayloadReceived;
        if (autoStart)
        {
            _reader.Start();
        }
    }

    public EntityId ReaderEntityId => _reader.ReaderEntityId;

    public int MatchedWriterCount => _reader.MatchedWriterCount;

    internal void SetAdvertiseTask(Task task) => _advertiseTask = task;

    private void OnPayloadReceived(ReadOnlyMemory<byte> payload, GuidPrefix sourcePrefix)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }
        _callback(payload, sourcePrefix);
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
}
