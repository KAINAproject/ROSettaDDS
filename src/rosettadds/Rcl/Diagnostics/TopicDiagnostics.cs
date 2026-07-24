using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using ROSettaDDS.Common;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rcl.Naming;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Rcl.Diagnostics
{
    public sealed class TopicDiagnostics : IDisposable
    {
        private readonly Node _node;
        private readonly Context _context;
        private bool _disposed;

        internal TopicDiagnostics(Node node)
        {
            _node = node ?? throw new ArgumentNullException(nameof(node));
            _context = node.Context;
        }

        public IReadOnlyList<TopicInfo> GetTopics()
        {
            ThrowIfDisposed();
            return Array.AsReadOnly(BuildTopicSnapshot());
        }

        public TopicInfo? GetTopicInfo(string topicName)
        {
            ThrowIfDisposed();
            if (topicName is null) throw new ArgumentException("Value cannot be null.", nameof(topicName));
            if (topicName.Length == 0) throw new ArgumentException("Value cannot be empty.", nameof(topicName));

            var topics = BuildTopicSnapshot();
            for (int i = 0; i < topics.Length; i++)
            {
                if (topics[i].TopicName == topicName)
                    return topics[i];
            }
            return null;
        }

        public TopicFrequencyMonitor CreateFrequencyMonitor(
            string topicName,
            TopicFrequencyOptions? options = null)
        {
            ThrowIfDisposed();
            if (string.IsNullOrEmpty(topicName))
                throw new ArgumentException("Value cannot be null or empty.", nameof(topicName));

            options ??= TopicFrequencyOptions.Default;

            var topicInfo = GetTopicInfo(topicName);
            if (topicInfo is null)
                throw new TopicNotFoundException(topicName);

            var ddsTypeNames = topicInfo.Endpoints
                .Select(e => e.DdsTypeName)
                .Where(n => !string.IsNullOrEmpty(n))
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            if (ddsTypeNames.Length != 1)
                throw new AmbiguousTopicTypeException(topicName);

            var ddsTypeName = ddsTypeNames[0];
            var ddsTopic = topicInfo.Endpoints[0].DdsTopicName;

            return new TopicFrequencyMonitor(_node, ddsTopic, ddsTypeName, options, SystemClock.Instance);
        }

        public void Dispose()
        {
            _disposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(GetType().Name);
            if (_node.IsDisposed) throw new ObjectDisposedException(GetType().Name);
        }

        private TopicInfo[] BuildTopicSnapshot()
        {
            var (snapshot, localGuids) = _context.CreateGraphSnapshotWithLocalInfo();

            var endpointInfos = new List<(string displayTopicName, TopicEndpointInfo info)>();

            for (int i = 0; i < snapshot.Endpoints.Count; i++)
            {
                var ep = snapshot.Endpoints[i];
                if (!ep.TopicName.StartsWith(TopicNameMangler.TopicPrefix, StringComparison.Ordinal))
                    continue;

                var displayTopicName = "/" + TopicNameMangler.DemangleTopic(ep.TopicName);
                var isLocal = localGuids.Contains(ep.EndpointGuid);
                var rosTypeName = string.IsNullOrEmpty(ep.TypeName)
                    ? null
                    : TypeNameMangler.DemangleType(ep.TypeName);

                var endpointInfo = new TopicEndpointInfo(
                    ep.EndpointGuid,
                    ep.Kind,
                    isLocal,
                    displayTopicName,
                    ep.TopicName,
                    ep.TypeName,
                    rosTypeName,
                    ep.Reliability,
                    ep.Durability);

                endpointInfos.Add((displayTopicName, endpointInfo));
            }

            if (endpointInfos.Count == 0)
                return Array.Empty<TopicInfo>();

            var groups = new Dictionary<string, GroupState>();
            for (int i = 0; i < endpointInfos.Count; i++)
            {
                var (displayName, info) = endpointInfos[i];
                if (!groups.TryGetValue(displayName, out var group))
                {
                    group = new GroupState();
                    groups[displayName] = group;
                }
                group.Endpoints.Add(info);
                if (info.RosTypeName is not null)
                    group.Types.Add(info.RosTypeName);
                if (info.Kind == EndpointKind.Writer)
                    group.PublisherCount++;
                else
                    group.SubscriberCount++;
            }

            var sortedKeys = groups.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();
            var result = new TopicInfo[sortedKeys.Length];
            for (int i = 0; i < sortedKeys.Length; i++)
            {
                var key = sortedKeys[i];
                var group = groups[key];
                result[i] = new TopicInfo(
                    key,
                    group.Types.ToArray(),
                    group.PublisherCount,
                    group.SubscriberCount,
                    group.Endpoints.ToArray());
            }

            return result;
        }

        private sealed class GroupState
        {
            public List<TopicEndpointInfo> Endpoints { get; } = new();
            public HashSet<string> Types { get; } = new();
            public int PublisherCount;
            public int SubscriberCount;
        }
    }
}
