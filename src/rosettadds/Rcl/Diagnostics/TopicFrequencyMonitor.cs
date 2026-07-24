using ROSettaDDS.Common;
using ROSettaDDS.Dds;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Rcl.Diagnostics;

public sealed class TopicFrequencyMonitor : IDisposable
{
    private readonly long[] _timestamps;
    private readonly int _windowSize;
    private readonly IClock _clock;
    private readonly RawSubscription? _rawSub;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private int _head;
    private int _count;
    private int _disposed;

    internal TopicFrequencyMonitor(TopicFrequencyOptions options, IClock clock)
    {
        _windowSize = options.WindowSize;
        _timestamps = new long[options.WindowSize];
        _clock = clock;
    }

    internal TopicFrequencyMonitor(
        Node node,
        string ddsTopic,
        string ddsTypeName,
        TopicFrequencyOptions options,
        IClock clock)
        : this(options, clock)
    {
        _rawSub = node.CreateRawReader(
            ddsTopic, ddsTypeName,
            OnPayload,
            options.Reliability, options.Durability);
    }

    internal int MatchedWriterCount => _rawSub?.MatchedWriterCount ?? 0;

    internal void Record(long timestamp)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            _timestamps[_head] = timestamp;
            _head = (_head + 1) % _windowSize;
            if (_count < int.MaxValue)
                _count++;
        }
    }

    public TopicFrequencyStatistics GetStatistics()
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            int totalCount = _count;
            if (totalCount == 0)
                return NoData(0);

            int actualCount = Math.Min(totalCount, _windowSize);
            if (actualCount < 2)
                return NoData(actualCount);

            var ordered = CollectOrderedTimestamps(actualCount);
            long first = ordered[0];
            long last = ordered[actualCount - 1];

            if (last <= first)
                return NoData(actualCount);

            var windowDuration = _clock.GetElapsedTime(first, last);
            if (windowDuration <= TimeSpan.Zero)
                return NoData(actualCount);
            int intervalCount = actualCount - 1;
            double rateHz = intervalCount / windowDuration.TotalSeconds;

            long minIntervalTicks = long.MaxValue;
            long maxIntervalTicks = long.MinValue;
            decimal sumIntervalTicks = 0;

            for (int i = 0; i < intervalCount; i++)
            {
                var interval = _clock.GetElapsedTime(ordered[i], ordered[i + 1]);
                long ticks = interval.Ticks;
                if (ticks < minIntervalTicks) minIntervalTicks = ticks;
                if (ticks > maxIntervalTicks) maxIntervalTicks = ticks;
                sumIntervalTicks += ticks;
            }

            double meanTicks = (double)(sumIntervalTicks / intervalCount);

            double sumSquaredDiffs = 0;
            for (int i = 0; i < intervalCount; i++)
            {
                var interval = _clock.GetElapsedTime(ordered[i], ordered[i + 1]);
                double diff = interval.Ticks - meanTicks;
                sumSquaredDiffs += diff * diff;
            }

            double stdDevTicks = Math.Sqrt(sumSquaredDiffs / intervalCount);

            return new TopicFrequencyStatistics(
                actualCount,
                true,
                rateHz,
                TimeSpan.FromTicks(minIntervalTicks),
                TimeSpan.FromTicks(maxIntervalTicks),
                TimeSpan.FromTicks((long)meanTicks),
                TimeSpan.FromTicks((long)stdDevTicks),
                windowDuration);
        }
    }

    public async Task<bool> WaitForMatchedAsync(int minCount, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (minCount <= 0) return true;
        if (timeout != System.Threading.Timeout.InfiniteTimeSpan && timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout));

        cancellationToken.ThrowIfCancellationRequested();

        var disposeToken = _disposeCts.Token;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, disposeToken);

        if (_rawSub is not null && _rawSub.MatchedWriterCount >= minCount)
            return true;

        if (timeout == TimeSpan.Zero)
            return false;

        var hasTimeout = timeout != System.Threading.Timeout.InfiniteTimeSpan && timeout != TimeSpan.MaxValue;
        long startTimestamp = 0;
        if (hasTimeout)
            startTimestamp = _clock.GetTimestamp();

        while (true)
        {
            if (hasTimeout && _clock.GetElapsedTime(startTimestamp, _clock.GetTimestamp()) >= timeout)
                return false;

            if (_rawSub is not null && _rawSub.MatchedWriterCount >= minCount)
                return true;

            try
            {
                await Task.Delay(10, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                ThrowIfDisposed();
                throw;
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _disposeCts.Cancel();
        _disposeCts.Dispose();
        _rawSub?.Dispose();
    }

    private void OnPayload(ReadOnlyMemory<byte> _, GuidPrefix __)
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;
        RecordCore(_clock.GetTimestamp());
    }

    private void RecordCore(long timestamp)
    {
        lock (_lock)
        {
            _timestamps[_head] = timestamp;
            _head = (_head + 1) % _windowSize;
            if (_count < int.MaxValue)
                _count++;
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(GetType().Name);
    }

    private static TopicFrequencyStatistics NoData(int sampleCount)
        => new(sampleCount, false, 0, TimeSpan.Zero, TimeSpan.Zero,
            TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);

    private long[] CollectOrderedTimestamps(int actualCount)
    {
        var ordered = new long[actualCount];
        if (_count < _windowSize)
        {
            Array.Copy(_timestamps, 0, ordered, 0, actualCount);
        }
        else
        {
            int tailCount = _windowSize - _head;
            Array.Copy(_timestamps, _head, ordered, 0, tailCount);
            Array.Copy(_timestamps, 0, ordered, tailCount, _head);
        }
        return ordered;
    }
}
