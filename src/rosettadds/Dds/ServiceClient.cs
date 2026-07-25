using System.Collections.Concurrent;
using System.IO;
using ROSettaDDS.Cdr;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Rtps.HistoryCache;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

/// <summary>
/// ROS 2 (Fast DDS) サービスのクライアント。request を <c>rq/&lt;svc&gt;Request</c> に publish し、
/// <c>rr/&lt;svc&gt;Reply</c> の reply を related_sample_identity で相関して返す。
/// </summary>
public sealed class ServiceClient<TRequest, TResponse> : IDisposable
{
    private readonly Publisher<TRequest> _requestPublisher;
    private readonly ReliableUserReader _replyReader;
    private readonly ServiceDescriptor<TRequest, TResponse> _descriptor;
    private readonly ILogger _logger;
    private readonly CdrReadLimits _cdrReadLimits;
    private readonly ConcurrentDictionary<SampleIdentity, TaskCompletionSource<TResponse>> _pending = new();
    private readonly object _pendingLock = new();
    private readonly Action<Guid, IUserReader>? _unregisterReplyEndpoint;
    internal Action? RemoveFromTracker { get; set; }
    private int _disposed;
    private Task? _replyReaderAdvertiseTask;
    private readonly ManualResetEventSlim _disposeCompleted = new();

    /// <summary>request writer の RTPS GUID。相関キーの writer 部に使う。</summary>
    public Guid RequestWriterGuid => _requestPublisher.Guid;

    internal ServiceClient(
        Publisher<TRequest> requestPublisher,
        ReliableUserReader replyReader,
        ServiceDescriptor<TRequest, TResponse> descriptor,
        ILogger logger,
        CdrReadLimits cdrReadLimits,
        Action<Guid, IUserReader>? unregisterReplyEndpoint = null)
    {
        _requestPublisher = requestPublisher;
        _replyReader = replyReader;
        _descriptor = descriptor;
        _logger = logger;
        _cdrReadLimits = cdrReadLimits;
        _unregisterReplyEndpoint = unregisterReplyEndpoint;
        _replyReader.SampleReceived += OnReplyReceived;
    }

    /// <summary>マッチするサービスサーバ (rq reader と rr writer) が見つかるまで待つ。</summary>
    public async Task<bool> WaitForServiceAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (IsServiceReady())
            {
                return true;
            }
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(20, cancellationToken).ConfigureAwait(false);
        }
        return IsServiceReady();
    }

    private bool IsServiceReady()
        => _requestPublisher.Writer.MatchedReaders.Count > 0
        && _replyReader.MatchedWriterCount > 0;

    /// <summary>request を送り、相関する response を待って返す。</summary>
    public async Task<TResponse> CallAsync(
        TRequest request, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var tcs = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        SampleIdentity key = default;
        bool added = false;
        try
        {
            await _requestPublisher.PublishReturningSequenceNumberAsync(
                request,
                assignedSn =>
                {
                    key = new SampleIdentity(_requestPublisher.Guid, assignedSn);
                    lock (_pendingLock)
                    {
                        if (_disposed != 0)
                            throw new ObjectDisposedException(nameof(ServiceClient<TRequest, TResponse>));
                        _pending[key] = tcs;
                    }
                    added = true;
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            if (added && _pending.TryRemove(key, out var removedTcs))
                removedTcs.TrySetCanceled(cancellationToken);
            throw;
        }
        catch (Exception ex)
        {
            if (added && _pending.TryRemove(key, out var removedTcs))
                removedTcs.TrySetException(ex);
            throw;
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);
        using (timeoutCts.Token.Register(static state =>
        {
            var ctx = ((ServiceClient<TRequest, TResponse> Client, SampleIdentity Key, TaskCompletionSource<TResponse> Tcs))state!;
            if (ctx.Client._pending.TryRemove(ctx.Key, out _))
            {
                ctx.Tcs.TrySetException(new TimeoutException(
                    $"Service call timed out after waiting for reply (sn={ctx.Key.SequenceNumber})."));
            }
        }, (this, key, tcs)))
        {
            try
            {
                return await tcs.Task.ConfigureAwait(false);
            }
            catch (TimeoutException) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
        }
    }

    private void OnReplyReceived(CacheChange change)
    {
        if (!Cdr.ParameterList.RelatedSampleIdentityInlineQos.TryRead(change.InlineQos.Span, change.InlineQosEndianness, out var related))
        {
            _logger.Debug("ServiceClient: reply without related_sample_identity; ignored");
            return;
        }
        if (!_pending.TryRemove(related, out var tcs))
        {
            _logger.Debug($"ServiceClient: reply for unknown request {related}; ignored");
            return;
        }
        try
        {
            var response = DeserializeResponse(change.SerializedPayload.Span);
            tcs.TrySetResult(response);
        }
        catch (Exception ex)
        {
            tcs.TrySetException(ex);
        }
    }

    private TResponse DeserializeResponse(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < CdrEncapsulation.Size)
        {
            throw new InvalidDataException(
                $"Reply payload too small for CDR encapsulation header (got {payload.Length} bytes).");
        }
        var (kind, _) = CdrEncapsulation.Read(payload[..CdrEncapsulation.Size]);
        var endian = CdrEncapsulation.GetEndianness(kind);
        var reader = new CdrReader(payload, endian, cdrOrigin: CdrEncapsulation.Size, limits: _cdrReadLimits);
        _descriptor.ResponseSerializer.Deserialize(ref reader, out var value);
        return value;
    }

    /// <summary>テスト用: reply を直接注入して相関ロジックを検証する。</summary>
    internal void InjectReplyForTest(CacheChange change) => OnReplyReceived(change);

    /// <summary>テスト用: reply reader の EntityId。</summary>
    internal EntityId ReplyReaderEntityIdForTest => _replyReader.ReaderEntityId;

    /// <summary>テスト用: 未解決の保留リクエスト数。</summary>
    internal int PendingRequestCount => _pending.Count;

    internal void SetReplyReaderAdvertiseTask(Task task) => _replyReaderAdvertiseTask = task;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            _disposeCompleted.Wait();
            return;
        }

        try
        {
            _replyReader.SampleReceived -= OnReplyReceived;

            KeyValuePair<SampleIdentity, TaskCompletionSource<TResponse>>[] pendingSnapshot;
            lock (_pendingLock)
            {
                pendingSnapshot = _pending.ToArray();
                _pending.Clear();
            }
            foreach (var kv in pendingSnapshot)
            {
                kv.Value.TrySetException(new ObjectDisposedException(nameof(ServiceClient<TRequest, TResponse>)));
            }

            if (_replyReaderAdvertiseTask is not null)
            {
                try { _replyReaderAdvertiseTask.ConfigureAwait(false).GetAwaiter().GetResult(); }
                catch { }
            }

            _replyReader.Stop();
            _unregisterReplyEndpoint?.Invoke(_replyReader.Guid, _replyReader);
            _requestPublisher.Dispose();
            _replyReader.Dispose();
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
