namespace ROSettaDDS.Rcl.Diagnostics;

public interface IClock
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
        var freq = System.Diagnostics.Stopwatch.Frequency;
        var ticks = (endingTimestamp - startingTimestamp) * TimeSpan.TicksPerSecond / freq;
        return TimeSpan.FromTicks(ticks);
    }
}
