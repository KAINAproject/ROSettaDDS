using ROSettaDDS.Common;
using ROSettaDDS.Dds.QoS;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Discovery;

/// <summary>
/// SEDP で交換される endpoint (Writer または Reader) の発見データ。
/// RTPS 仕様 9.6.2.2.4 / 9.6.2.2.5 (DiscoveredWriterData / DiscoveredReaderData)。
/// 共通フィールドのみを保持し、Writer/Reader の区別は <see cref="Kind"/> で表現する。
/// </summary>
public sealed class DiscoveredEndpointData
{
    public EndpointKind Kind { get; set; }

    /// <summary>endpoint の Guid (PID_ENDPOINT_GUID)。必須。</summary>
    public Guid EndpointGuid { get; set; }

    /// <summary>endpoint を所有する Participant の Guid (PID_PARTICIPANT_GUID)。必須。</summary>
    public Guid ParticipantGuid { get; set; }

    /// <summary>マングル済みトピック名 (PID_TOPIC_NAME)、例 "rt/chatter"。必須。</summary>
    public string TopicName { get; set; } = "";

    /// <summary>マングル済み型名 (PID_TYPE_NAME)、例 "std_msgs::msg::dds_::String_"。必須。</summary>
    public string TypeName { get; set; } = "";

    public ReliabilityQos Reliability { get; set; } = ReliabilityQos.BestEffort;
    public DurabilityQos Durability { get; set; } = DurabilityQos.Volatile;
    public DeadlineQos Deadline { get; set; } = DeadlineQos.Default;
    public LatencyBudgetQos LatencyBudget { get; set; } = LatencyBudgetQos.Default;
    public LivelinessQos Liveliness { get; set; } = LivelinessQos.Default;
    public OwnershipQos Ownership { get; set; } = OwnershipQos.Default;
    public DestinationOrderQos DestinationOrder { get; set; } = DestinationOrderQos.Default;
    public PresentationQos Presentation { get; set; } = PresentationQos.Default;
    public PartitionQos Partition { get; set; } = PartitionQos.Default;

    /// <summary>endpoint の unicast ロケータ (PID_UNICAST_LOCATOR)。FastDDS はこれを必要とする。</summary>
    public List<Locator> UnicastLocators { get; } = new();

    /// <summary>endpoint の multicast ロケータ (PID_MULTICAST_LOCATOR)。</summary>
    public List<Locator> MulticastLocators { get; } = new();

    internal DiscoveredEndpointData Clone()
    {
        var clone = new DiscoveredEndpointData
        {
            Kind = Kind,
            EndpointGuid = EndpointGuid,
            ParticipantGuid = ParticipantGuid,
            TopicName = TopicName,
            TypeName = TypeName,
            Reliability = Reliability,
            Durability = Durability,
            Deadline = Deadline,
            LatencyBudget = LatencyBudget,
            Liveliness = Liveliness,
            Ownership = Ownership,
            DestinationOrder = DestinationOrder,
            Presentation = Presentation,
            Partition = new PartitionQos(Partition.Names.ToArray()),
        };
        clone.UnicastLocators.AddRange(UnicastLocators);
        clone.MulticastLocators.AddRange(MulticastLocators);
        return clone;
    }
}
