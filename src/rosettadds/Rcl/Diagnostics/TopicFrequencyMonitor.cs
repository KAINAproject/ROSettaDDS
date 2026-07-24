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

    public int MatchedWriterCount => _rawSub?.MatchedWriterCount ?? 0;

    public void Record(long timestamp)
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;
        lock (_lock)
        {
            _timestamps[_head] = timestamp;
            _head = (_head + 1) % _windowSize;
            _count++;
        }
    }

    public TopicFrequencyStatistics ComputeStatistics()
    {
        lock (_lock)
        {
            int sampleCount = _count;
            if (sampleCount == 0)
                return NoData(sampleCount);

            int actualCount = Math.Min(sampleCount, _windowSize);
            if (actualCount < 2)
                return NoData(sampleCount);

            var ordered = CollectOrderedTimestamps(actualCount);
            long first = ordered[0];
            long last = ordered[actualCount - 1];

            if (last <= first)
                return NoData(sampleCount);

            var duration = _clock.GetElapsedTime(first, last);
            int intervalCount = actualCount - 1;
            double rateHz = intervalCount / duration.TotalSeconds;

            long minIntervalTicks = long.MaxValue;
            long maxIntervalTicks = long.MinValue;
            long sumIntervalTicks = 0;

            for (int i = 0; i < intervalCount; i++)
            {
                var interval = _clock.GetElapsedTime(ordered[i], ordered[i + 1]);
                long ticks = interval.Ticks;
                if (ticks < minIntervalTicks) minIntervalTicks = ticks;
                if (ticks > maxIntervalTicks) maxIntervalTicks = ticks;
                sumIntervalTicks += ticks;
            }

            double meanTicks = (double)sumIntervalTicks / intervalCount;

            double sumSquaredDiffs = 0;
            for (int i = 0; i < intervalCount; i++)
            {
                var interval = _clock.GetElapsedTime(ordered[i], ordered[i + 1]);
                double diff = interval.Ticks - meanTicks;
                sumSquaredDiffs += diff * diff;
            }

            double stdDevTicks = Math.Sqrt(sumSquaredDiffs / intervalCount);

            return new TopicFrequencyStatistics(
                sampleCount,
                true,
                rateHz,
                TimeSpan.FromTicks(minIntervalTicks),
                TimeSpan.FromTicks(maxIntervalTicks),
                TimeSpan.FromTicks((long)meanTicks),
                TimeSpan.FromTicks((long)stdDevTicks),
                duration);
        }
    }

    public async Task<bool> WaitForMatchedAsync(int minCount, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);
        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                if (_rawSub is not null && _rawSub.MatchedWriterCount >= minCount)
                    return true;
                try
                {
                    await Task.Delay(10, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        cancellationToken.ThrowIfCancellationRequested();
        return false;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _rawSub?.Dispose();
    }

    private void OnPayload(ReadOnlyMemory<byte> _, GuidPrefix __)
    {
        Record(_clock.GetTimestamp());
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
