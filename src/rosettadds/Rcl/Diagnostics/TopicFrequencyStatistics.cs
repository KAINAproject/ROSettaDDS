namespace ROSettaDDS.Rcl.Diagnostics;

public sealed class TopicFrequencyStatistics
{
    public int SampleCount { get; }
    public bool HasData { get; }
    public double RateHz { get; }
    public TimeSpan MinInterval { get; }
    public TimeSpan MaxInterval { get; }
    public TimeSpan MeanInterval { get; }
    public TimeSpan StdDevInterval { get; }
    public TimeSpan Duration { get; }

    internal TopicFrequencyStatistics(
        int sampleCount,
        bool hasData,
        double rateHz,
        TimeSpan minInterval,
        TimeSpan maxInterval,
        TimeSpan meanInterval,
        TimeSpan stdDevInterval,
        TimeSpan duration)
    {
        SampleCount = sampleCount;
        HasData = hasData;
        RateHz = rateHz;
        MinInterval = minInterval;
        MaxInterval = maxInterval;
        MeanInterval = meanInterval;
        StdDevInterval = stdDevInterval;
        Duration = duration;
    }
}
