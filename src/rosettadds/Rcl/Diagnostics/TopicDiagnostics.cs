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
        private readonly List<TopicFrequencyMonitor> _monitors = new();
        private readonly object _monitorsLock = new();
        private int _disposed;
        private readonly ManualResetEventSlim _disposeCompletedGate = new();
        private Exception? _disposeException;

        /// <summary>Test seam: records lifecycle events for verification.</summary>
    internal Action<string>? TestEventRecorder { get; set; }
    internal Action? RemoveFromTracker { get; set; }

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
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            if (ddsTypeNames.Length != 1 || string.IsNullOrEmpty(ddsTypeNames[0]))
                throw new AmbiguousTopicTypeException(topicName);

            var ddsTypeName = ddsTypeNames[0];
            var ddsTopic = topicInfo.Endpoints[0].DdsTopicName;

            var monitor = new TopicFrequencyMonitor(_node, ddsTopic, ddsTypeName, options, SystemClock.Instance);
            monitor.RemoveFromTracker = () => { lock (_monitorsLock) _monitors.Remove(monitor); };
            lock (_monitorsLock)
            {
                if (_disposed != 0 || _node.IsDisposed)
                {
                    monitor.Dispose();
                    ThrowIfDisposed();
                }
                _monitors.Add(monitor);
            }
            return monitor;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                _disposeCompletedGate.Wait();
                if (_disposeException is not null)
                {
                    throw new AggregateException(
                        "TopicDiagnostics.Dispose failed on the first call and is propagated here.",
                        _disposeException);
                }
                return;
            }

            try
            {
                RemoveFromTracker?.Invoke();
                TestEventRecorder?.Invoke("TopicDiagnosticsDisposeStart");
                TopicFrequencyMonitor[] snapshot;
                lock (_monitorsLock)
                {
                    snapshot = _monitors.ToArray();
                    _monitors.Clear();
                }
                foreach (var m in snapshot)
                    m.Dispose();
                TestEventRecorder?.Invoke("TopicDiagnosticsDisposeEnd");
            }
            catch (Exception ex)
            {
                _disposeException = ex;
                throw;
            }
            finally
            {
                _disposeCompletedGate.Set();
            }
        }
        }

        private void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(GetType().Name);
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
