using ROSettaDDS.Dds.QoS;

namespace ROSettaDDS.Rcl.Diagnostics;

public sealed class TopicFrequencyOptions
{
    public const int DefaultWindowSize = 10000;
    public const int MaxWindowSize = 1_000_000;

    public int WindowSize { get; }
    public ReliabilityQos Reliability { get; }
    public DurabilityQos Durability { get; }

    public TopicFrequencyOptions()
        : this(DefaultWindowSize, ReliabilityQos.BestEffort, DurabilityQos.Volatile)
    {
    }

    public TopicFrequencyOptions(int windowSize, ReliabilityQos? reliability = null, DurabilityQos? durability = null)
    {
        if (windowSize is < 2 or > MaxWindowSize)
            throw new ArgumentOutOfRangeException(
                nameof(windowSize), windowSize,
                $"WindowSize must be between 2 and {MaxWindowSize}.");
        WindowSize = windowSize;
        Reliability = reliability ?? ReliabilityQos.BestEffort;
        Durability = durability ?? DurabilityQos.Volatile;
    }

    public static TopicFrequencyOptions Default { get; } = new();
}
