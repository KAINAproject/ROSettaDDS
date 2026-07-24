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
        double seconds = (double)(endingTimestamp - startingTimestamp) / freq;
        return TimeSpan.FromSeconds(seconds);
    }
}
