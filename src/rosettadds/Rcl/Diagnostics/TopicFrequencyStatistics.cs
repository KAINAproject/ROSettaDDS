namespace ROSettaDDS.Rcl.Diagnostics;

public sealed class TopicFrequencyStatistics
{
    public int SampleCount { get; }
    public bool HasData { get; }
    public double RateHz { get; }
    public TimeSpan MinInterval { get; }
    public TimeSpan MaxInterval { get; }
    public TimeSpan MeanInterval { get; }
    public TimeSpan StandardDeviation { get; }
    public TimeSpan WindowDuration { get; }

    internal TopicFrequencyStatistics(
        int sampleCount,
        bool hasData,
        double rateHz,
        TimeSpan minInterval,
        TimeSpan maxInterval,
        TimeSpan meanInterval,
        TimeSpan standardDeviation,
        TimeSpan windowDuration)
    {
        SampleCount = sampleCount;
        HasData = hasData;
        RateHz = rateHz;
        MinInterval = minInterval;
        MaxInterval = maxInterval;
        MeanInterval = meanInterval;
        StandardDeviation = standardDeviation;
        WindowDuration = windowDuration;
    }
}
