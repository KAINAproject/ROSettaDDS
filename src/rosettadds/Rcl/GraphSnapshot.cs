using ROSettaDDS.Common;
using ROSettaDDS.Discovery;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Rcl;

internal readonly record struct GraphSnapshot(IReadOnlyList<DiscoveredEndpointData> Endpoints)
{
    public static GraphSnapshot Empty { get; } = new(Array.Empty<DiscoveredEndpointData>());
}
