using ROSettaDDS.Cdr;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Naming;
using ROSettaDDS.Rtps.HistoryCache;
using ROSettaDDS.Rtps.Reader;
using ROSettaDDS.Rtps.Writer;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Dds;

internal sealed class ParticipantEndpointFactory
{
    private readonly Rcl.ContextOptions _options;
    private readonly ParticipantTransportSet _transports;
    private readonly GuidPrefix _guidPrefix;
    private readonly Guid _participantGuid;
    private readonly UserEntityIdAllocator _entityIds;
    private readonly ILogger _logger;

    public ParticipantEndpointFactory(
        Rcl.ContextOptions options,
        ParticipantTransportSet transports,
        GuidPrefix guidPrefix,
        Guid participantGuid,
        UserEntityIdAllocator entityIds)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _transports = transports ?? throw new ArgumentNullException(nameof(transports));
        _guidPrefix = guidPrefix;
        _participantGuid = participantGuid;
        _entityIds = entityIds ?? throw new ArgumentNullException(nameof(entityIds));
        _logger = options.Logger;
    }

    public WriterEndpoint CreateWriter<T>(
        string ddsTopic,
        ICdrSerializer<T> serializer,
        ReliabilityQos reliability,
        DurabilityQos durability,
        string? typeName)
    {
        if (string.IsNullOrEmpty(ddsTopic)) throw new ArgumentException("Value cannot be null or empty.", nameof(ddsTopic));
        if (serializer is null) throw new ArgumentNullException(nameof(serializer));

        var writerEntityId = _entityIds.AllocateWriter();
        var writerGuid = new Guid(_guidPrefix, writerEntityId);
        var history = new WriterHistoryCache(writerGuid, maxSamples: _options.UserWriterHistoryDepth);
        var writer = new StatefulWriter(
            sendTransport: _transports.UserUnicast,
            multicastDestination: _transports.UserMulticastDestination,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: _guidPrefix,
            writerEntityId: writerEntityId,
            heartbeatPeriod: _options.UserWriterHeartbeatPeriod,
            history: history,
            logger: _logger,
            resendHistoryOnMatch: durability.Kind == DurabilityKind.TransientLocal);

        var endpointData = CreateEndpointData(
            EndpointKind.Writer,
            writerGuid,
            ddsTopic,
            ResolveDdsTypeName<T>(typeName),
            reliability,
            durability);

        return new WriterEndpoint(writer, endpointData);
    }

    public ReaderEndpoint CreateReader<T>(
        string ddsTopic,
        ICdrSerializer<T> serializer,
        ReliabilityQos reliability,
        string? typeName)
    {
        if (string.IsNullOrEmpty(ddsTopic)) throw new ArgumentException("Value cannot be null or empty.", nameof(ddsTopic));
        if (serializer is null) throw new ArgumentNullException(nameof(serializer));

        var readerEntityId = _entityIds.AllocateReader();
        var endpointGuid = new Guid(_guidPrefix, readerEntityId);
        IUserReader reader = reliability.Kind == ReliabilityKind.Reliable
            ? CreateReliableReader(readerEntityId)
            : new BestEffortUserReader(_guidPrefix, readerEntityId, _logger, _options.DataFragReassembly);

        var endpointData = CreateEndpointData(
            EndpointKind.Reader,
            endpointGuid,
            ddsTopic,
            ResolveDdsTypeName<T>(typeName),
            reliability,
            DurabilityQos.Volatile);

        return new ReaderEndpoint(reader, endpointGuid, endpointData);
    }

    public ReaderEndpoint CreateRawReader(
        string ddsTopic,
        string ddsTypeName,
        ReliabilityQos reliability,
        DurabilityQos durability)
    {
        if (string.IsNullOrEmpty(ddsTopic)) throw new ArgumentException("Value cannot be null or empty.", nameof(ddsTopic));
        if (string.IsNullOrEmpty(ddsTypeName)) throw new ArgumentException("Value cannot be null or empty.", nameof(ddsTypeName));

        var readerEntityId = _entityIds.AllocateReader();
        var endpointGuid = new Guid(_guidPrefix, readerEntityId);
        IUserReader reader = reliability.Kind == ReliabilityKind.Reliable
            ? CreateReliableReader(readerEntityId)
            : new BestEffortUserReader(_guidPrefix, readerEntityId, _logger, _options.DataFragReassembly);

        var endpointData = CreateEndpointData(
            EndpointKind.Reader,
            endpointGuid,
            ddsTopic,
            ddsTypeName,
            reliability,
            durability);

        return new ReaderEndpoint(reader, endpointGuid, endpointData);
    }

    public ReliableReaderEndpoint CreateReliableReplyReader(string ddsTopic, string ddsTypeName)
    {
        if (string.IsNullOrEmpty(ddsTopic)) throw new ArgumentException("Value cannot be null or empty.", nameof(ddsTopic));
        if (string.IsNullOrEmpty(ddsTypeName)) throw new ArgumentException("Value cannot be null or empty.", nameof(ddsTypeName));

        var readerEntityId = _entityIds.AllocateReader();
        var endpointGuid = new Guid(_guidPrefix, readerEntityId);
        var reader = CreateReliableReader(readerEntityId);
        var endpointData = CreateEndpointData(
            EndpointKind.Reader,
            endpointGuid,
            ddsTopic,
            ddsTypeName,
            ReliabilityQos.Reliable,
            DurabilityQos.Volatile);

        return new ReliableReaderEndpoint(reader, endpointGuid, endpointData);
    }

    private ReliableUserReader CreateReliableReader(EntityId readerEntityId)
        => new(
            replyTransport: _transports.UserUnicast,
            version: _options.ProtocolVersion,
            vendorId: _options.VendorId,
            localPrefix: _guidPrefix,
            readerEntityId: readerEntityId,
            ackNackFallbackDestination: _transports.UserMulticastDestination,
            logger: _logger,
            dataFragOptions: _options.DataFragReassembly);

    private DiscoveredEndpointData CreateEndpointData(
        EndpointKind kind,
        Guid endpointGuid,
        string ddsTopic,
        string ddsTypeName,
        ReliabilityQos reliability,
        DurabilityQos durability)
    {
        var endpointData = new DiscoveredEndpointData
        {
            Kind = kind,
            EndpointGuid = endpointGuid,
            ParticipantGuid = _participantGuid,
            TopicName = ddsTopic,
            TypeName = ddsTypeName,
            Reliability = reliability,
            Durability = durability,
        };
        endpointData.UnicastLocators.AddRange(_transports.DefaultUnicastLocators);
        endpointData.MulticastLocators.Add(_transports.UserMulticastDestination);
        return endpointData;
    }

    internal static string ResolveDdsTypeName<T>(string? explicitTypeName)
    {
        if (!string.IsNullOrEmpty(explicitTypeName))
        {
            return explicitTypeName;
        }

        var field = typeof(T).GetField(
            "DdsTypeName",
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        return field?.GetRawConstantValue() as string ?? "";
    }

    public sealed record WriterEndpoint(StatefulWriter Writer, DiscoveredEndpointData EndpointData);
    public sealed record ReaderEndpoint(IUserReader Reader, Guid EndpointGuid, DiscoveredEndpointData EndpointData);
    public sealed record ReliableReaderEndpoint(ReliableUserReader Reader, Guid EndpointGuid, DiscoveredEndpointData EndpointData);
}
