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
        var result = TicksToTimeSpan((long)delta, System.Diagnostics.Stopwatch.Frequency);
        if (result <= TimeSpan.Zero)
            return TimeSpan.Zero;
        return result;
    }

    internal static TimeSpan TicksToTimeSpan(long ticks, long frequency)
    {
        if (ticks <= 0 || frequency <= 0)
            return TimeSpan.Zero;
        decimal seconds = (decimal)ticks / frequency;
        if (seconds <= 0)
            return TimeSpan.Zero;
        long resultTicks = (long)(seconds * TimeSpan.TicksPerSecond);
        if (resultTicks <= 0)
            return TimeSpan.Zero;
        return TimeSpan.FromTicks(resultTicks);
    }
}
