using System.Linq;
using ROSettaDDS.Common;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rtps.Writer;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

internal sealed class EndpointRegistry
{
    private readonly object _lock = new();
    private readonly List<DiscoveredEndpointData> _writers = new();
    private readonly List<DiscoveredEndpointData> _readers = new();
    private readonly Dictionary<string, List<LocalWriter>> _writersByTopic = new();
    private readonly Dictionary<string, List<LocalReader>> _readersByTopic = new();
    private StatefulWriter[] _writerSnapshot = Array.Empty<StatefulWriter>();

    public void AddLocalWriter(DiscoveredEndpointData endpoint, StatefulWriter writer)
    {
        lock (_lock)
        {
            _writers.Add(endpoint);
            AddByTopic(_writersByTopic, endpoint.TopicName, new LocalWriter(endpoint, writer));
            RefreshWriterSnapshotLocked();
        }
    }

    public void AddLocalReader(DiscoveredEndpointData endpoint, IUserReader reader)
    {
        lock (_lock)
        {
            _readers.Add(endpoint);
            AddByTopic(_readersByTopic, endpoint.TopicName, new LocalReader(endpoint, reader));
        }
    }

    public RemovedWriter RemoveLocalWriter(Guid endpointGuid, StatefulWriter writer)
    {
        lock (_lock)
        {
            var endpoint = RemoveEndpoint(_writers, endpointGuid);
            if (endpoint is null) return new RemovedWriter(null, Array.Empty<LocalReader>());
            RemoveByReference(_writersByTopic, endpoint.TopicName, writer, static item => item.Writer);
            RefreshWriterSnapshotLocked();
            var readers = SnapshotForTopic(_readersByTopic, endpoint.TopicName);
            return new RemovedWriter(endpoint, readers);
        }
    }

    public RemovedReader RemoveLocalReader(Guid endpointGuid, IUserReader reader)
    {
        lock (_lock)
        {
            var endpoint = RemoveEndpoint(_readers, endpointGuid);
            if (endpoint is null) return new RemovedReader(null, Array.Empty<LocalWriter>());
            RemoveByReference(_readersByTopic, endpoint.TopicName, reader, static item => item.Reader);
            var writers = SnapshotForTopic(_writersByTopic, endpoint.TopicName);
            return new RemovedReader(endpoint, writers);
        }
    }

    public readonly record struct RemovedWriter(DiscoveredEndpointData? Endpoint, LocalReader[] LocalReaders);
    public readonly record struct RemovedReader(DiscoveredEndpointData? Endpoint, LocalWriter[] LocalWriters);

    public bool ShouldAdvertiseForTopic(string topicName, Guid removedEndpointGuid)
    {
        lock (_lock)
        {
            return !ContainsGuid(_writersByTopic, topicName, removedEndpointGuid, static item => item.EndpointData)
                && !ContainsGuid(_readersByTopic, topicName, removedEndpointGuid, static item => item.EndpointData);
        }
    }

    public LocalWriter[] GetLocalWritersForTopic(string topicName)
    {
        lock (_lock) return SnapshotForTopic(_writersByTopic, topicName);
    }

    public LocalReader[] GetLocalReadersForTopic(string topicName)
    {
        lock (_lock) return SnapshotForTopic(_readersByTopic, topicName);
    }

    public void StartWriters()
    {
        foreach (var w in Volatile.Read(ref _writerSnapshot)) w.Start();
    }

    public void StopWriters()
    {
        foreach (var w in Volatile.Read(ref _writerSnapshot)) w.Stop();
    }

    public EndpointSnapshot Snapshot()
    {
        lock (_lock)
        {
            return new EndpointSnapshot(
                _writersByTopic.Values.SelectMany(static items => items).Select(static item => item.Writer).ToArray(),
                _readersByTopic.Values.SelectMany(static items => items).Select(static item => item.Reader).ToArray());
        }
    }

    /// <summary>現在の全 local endpoint metadata を値コピーで返す。</summary>
    internal EndpointDiscoverySnapshot LocalEndpointSnapshot()
    {
        lock (_lock)
        {
            return new EndpointDiscoverySnapshot(
                _writers.Select(static w => w.Clone()).ToArray(),
                _readers.Select(static r => r.Clone()).ToArray());
        }
    }

    public EndpointDiscoverySnapshot UpdateLocalLocators(
        IReadOnlyList<Locator> unicastLocators,
        Locator multicastLocator)
    {
        if (unicastLocators is null) throw new ArgumentNullException(nameof(unicastLocators));

        lock (_lock)
        {
            foreach (var endpoint in _writers)
            {
                UpdateEndpointLocators(endpoint, unicastLocators, multicastLocator);
            }
            foreach (var endpoint in _readers)
            {
                UpdateEndpointLocators(endpoint, unicastLocators, multicastLocator);
            }
            return new EndpointDiscoverySnapshot(_writers.ToArray(), _readers.ToArray());
        }
    }

    private void RefreshWriterSnapshotLocked()
    {
        _writerSnapshot = _writersByTopic.Values
            .SelectMany(static items => items)
            .Select(static item => item.Writer)
            .ToArray();
    }

    private static void UpdateEndpointLocators(
        DiscoveredEndpointData endpoint,
        IReadOnlyList<Locator> unicastLocators,
        Locator multicastLocator)
    {
        endpoint.UnicastLocators.Clear();
        endpoint.UnicastLocators.AddRange(unicastLocators);
        endpoint.MulticastLocators.Clear();
        endpoint.MulticastLocators.Add(multicastLocator);
    }

    private static void AddByTopic<T>(Dictionary<string, List<T>> map, string topic, T item)
    {
        if (!map.TryGetValue(topic, out var list))
        {
            list = new List<T>();
            map[topic] = list;
        }
        list.Add(item);
    }

    private static T[] SnapshotForTopic<T>(Dictionary<string, List<T>> map, string topic)
        => map.TryGetValue(topic, out var list) ? list.ToArray() : Array.Empty<T>();

    private static DiscoveredEndpointData? RemoveEndpoint(List<DiscoveredEndpointData> endpoints, Guid endpointGuid)
    {
        for (int i = 0; i < endpoints.Count; i++)
        {
            if (endpoints[i].EndpointGuid.Equals(endpointGuid))
            {
                var ep = endpoints[i];
                endpoints.RemoveAt(i);
                return ep;
            }
        }
        return null;
    }

    private static bool RemoveByReference<TItem, TValue>(
        Dictionary<string, List<TItem>> map, string topic, TValue value, Func<TItem, TValue> selector)
        where TValue : class
    {
        if (!map.TryGetValue(topic, out var list)) return false;
        for (int i = 0; i < list.Count; i++)
        {
            if (ReferenceEquals(selector(list[i]), value))
            {
                list.RemoveAt(i);
                if (list.Count == 0) map.Remove(topic);
                return true;
            }
        }
        return false;
    }

    private static bool ContainsGuid<T>(
        Dictionary<string, List<T>> itemsByTopic,
        string topicName,
        Guid endpointGuid,
        Func<T, DiscoveredEndpointData> selector)
        => itemsByTopic.TryGetValue(topicName, out var items)
        && items.Any(item => selector(item).EndpointGuid.Equals(endpointGuid));
}
