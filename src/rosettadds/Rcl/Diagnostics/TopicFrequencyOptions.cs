using ROSettaDDS.Dds.QoS;

namespace ROSettaDDS.Rcl.Diagnostics;

public sealed class TopicFrequencyOptions
{
    public const int DefaultWindowSize = 10000;
    public const int MaxWindowSize = 1_000_000;

    private int _windowSize = DefaultWindowSize;

    public int WindowSize
    {
        get => _windowSize;
        init
        {
            if (value is < 2 or > MaxWindowSize)
                throw new ArgumentOutOfRangeException(
                    nameof(WindowSize), value,
                    $"WindowSize must be between 2 and {MaxWindowSize}.");
            _windowSize = value;
        }
    }

    public ReliabilityQos Reliability { get; init; } = ReliabilityQos.BestEffort;
    public DurabilityQos Durability { get; init; } = DurabilityQos.Volatile;

    public static TopicFrequencyOptions Default { get; } = new();
}
