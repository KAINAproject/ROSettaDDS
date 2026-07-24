namespace ROSettaDDS.Rcl.Diagnostics;

internal interface IClock
{
    long GetTimestamp();
    TimeSpan GetElapsedTime(long startingTimestamp, long endingTimestamp);
}

internal sealed class SystemClock : IClock
{
    public static readonly SystemClock Instance = new();

    public long GetTimestamp() => System.Diagnostics.Stopwatch.GetTimestamp();

    public TimeSpan GetElapsedTime(long startingTimestamp, long endingTimestamp)
    {
        if (endingTimestamp < startingTimestamp)
            return TimeSpan.Zero;
        decimal delta = (decimal)endingTimestamp - (decimal)startingTimestamp;
        if (delta <= 0)
            return TimeSpan.Zero;
        decimal seconds = delta / System.Diagnostics.Stopwatch.Frequency;
        if (seconds <= 0)
            return TimeSpan.Zero;
        long ticks = (long)(seconds * TimeSpan.TicksPerSecond);
        if (ticks <= 0)
            return TimeSpan.Zero;
        return TimeSpan.FromTicks(ticks);
    }
}
