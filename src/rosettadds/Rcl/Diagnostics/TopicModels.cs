using System.Collections.Generic;
using System.Collections.ObjectModel;
using ROSettaDDS.Common;
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Discovery;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Rcl.Diagnostics
{
    public sealed class TopicEndpointInfo
    {
        public Guid EndpointGuid { get; }
        public EndpointKind Kind { get; }
        public bool IsLocal { get; }
        public string TopicName { get; }
        public string DdsTopicName { get; }
        public string DdsTypeName { get; }
        public string? RosTypeName { get; }
        public ReliabilityQos Reliability { get; }
        public DurabilityQos Durability { get; }

        internal TopicEndpointInfo(
            Guid endpointGuid,
            EndpointKind kind,
            bool isLocal,
            string topicName,
            string ddsTopicName,
            string ddsTypeName,
            string? rosTypeName,
            ReliabilityQos reliability,
            DurabilityQos durability)
        {
            EndpointGuid = endpointGuid;
            Kind = kind;
            IsLocal = isLocal;
            TopicName = topicName;
            DdsTopicName = ddsTopicName;
            DdsTypeName = ddsTypeName;
            RosTypeName = rosTypeName;
            Reliability = reliability;
            Durability = durability;
        }
    }

    public sealed class TopicInfo
    {
        public string TopicName { get; }
        public IReadOnlyList<string> RosTypeNames { get; }
        public int PublisherCount { get; }
        public int SubscriberCount { get; }
        public IReadOnlyList<TopicEndpointInfo> Endpoints { get; }

        internal TopicInfo(
            string topicName,
            string[] rosTypeNames,
            int publisherCount,
            int subscriberCount,
            TopicEndpointInfo[] endpoints)
        {
            TopicName = topicName;
            RosTypeNames = Array.AsReadOnly(rosTypeNames);
            PublisherCount = publisherCount;
            SubscriberCount = subscriberCount;
            Endpoints = Array.AsReadOnly(endpoints);
        }
    }
}
