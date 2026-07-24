using ROSettaDDS.Dds.QoS;

namespace ROSettaDDS.Rcl.Diagnostics;

public sealed class TopicFrequencyOptions
{
    internal const int DefaultWindowSize = 10000;
    private const int MaxWindowSize = 1_000_000;

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

    internal static TopicFrequencyOptions Default { get; } = new();
}
