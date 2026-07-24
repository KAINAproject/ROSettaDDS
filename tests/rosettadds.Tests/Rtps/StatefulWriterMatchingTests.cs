using System.Diagnostics;
using System.Net;
using ROSettaDDS.Common;
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rtps.HistoryCache;
using ROSettaDDS.Rtps.Submessages;
using ROSettaDDS.Rtps.Writer;
using ROSettaDDS.Transport;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Tests.Rtps;

public class StatefulWriterMatchingTests
{
    private sealed class Setup
    {
        public required LoopbackHub Hub { get; init; }
        public required LoopbackTransport WriterTransport { get; init; }
        public required LoopbackTransport ReaderTransport { get; init; }
        public required Locator WriterLocator { get; init; }
        public required Locator ReaderLocator { get; init; }
        public required GuidPrefix WriterPrefix { get; init; }
        public required GuidPrefix ReaderPrefix { get; init; }
        public required EntityId WriterEntityId { get; init; }
        public required EntityId ReaderEntityId { get; init; }
    }

    private static Setup CreateSetup()
    {
        var hub = new LoopbackHub();
        var writerLoc = Locator.FromUdpV4(IPAddress.Parse("10.0.0.1"), 7411u);
        var readerLoc = Locator.FromUdpV4(IPAddress.Parse("10.0.0.2"), 7413u);
        var writerTr = hub.Create(writerLoc);
        var readerTr = hub.Create(readerLoc);
        return new Setup
        {
            Hub = hub,
            WriterTransport = writerTr,
            ReaderTransport = readerTr,
            WriterLocator = writerLoc,
            ReaderLocator = readerLoc,
            WriterPrefix = GuidPrefix.Create(VendorId.ROSettaDDS, 0x11, 0x22, 0x01),
            ReaderPrefix = GuidPrefix.Create(VendorId.ROSettaDDS, 0x11, 0x22, 0x02),
            WriterEntityId = new EntityId(0x0000_0001u, EntityKind.UserDefinedWriterNoKey),
            ReaderEntityId = new EntityId(0x0000_0002u, EntityKind.UserDefinedReaderNoKey),
        };
    }

    private static StatefulWriter CreateWriter(Setup s, out WriterHistoryCache history)
    {
        var writerGuid = new Guid(s.WriterPrefix, s.WriterEntityId);
        history = new WriterHistoryCache(writerGuid);
        return new StatefulWriter(
            sendTransport: s.WriterTransport,
            multicastDestination: s.ReaderLocator,
            version: ProtocolVersion.V2_4,
            vendorId: VendorId.ROSettaDDS,
            localPrefix: s.WriterPrefix,
            writerEntityId: s.WriterEntityId,
            heartbeatPeriod: TimeSpan.FromMilliseconds(50),
            history: history);
    }

    [Fact]
    public void duplicate_matchは累積件数を増やさずLocatorだけ更新する()
    {
        var s = CreateSetup();
        var readerGuid = new Guid(s.ReaderPrefix, s.ReaderEntityId);
        using var writer = CreateWriter(s, out _);
        var firstLocator = Locator.FromUdpV4(IPAddress.Parse("10.0.0.20"), 8000u);
        var updatedLocator = Locator.FromUdpV4(IPAddress.Parse("10.0.0.21"), 8001u);

        writer.MatchReader(readerGuid, firstLocator, ReliabilityKind.Reliable);
        writer.MatchReader(readerGuid, updatedLocator, ReliabilityKind.Reliable);

        writer.MatchedReaderCount.Should().Be(1);
        writer.GetReaderProxy(readerGuid)!.UnicastLocator.Should().Be(updatedLocator);
        var status = writer.PublicationMatchedStatus;
        status.CurrentCount.Should().Be(1);
        status.TotalCount.Should().Be(1);
    }

    [Fact]
    public void unmatch_and_rematchは現在件数と累積件数を正しく更新する()
    {
        var s = CreateSetup();
        var readerGuid = new Guid(s.ReaderPrefix, s.ReaderEntityId);
        using var writer = CreateWriter(s, out _);

        writer.MatchReader(readerGuid, s.ReaderLocator);
        writer.MatchedReaderCount.Should().Be(1);
        var statusAfterMatch = writer.PublicationMatchedStatus;
        statusAfterMatch.CurrentCount.Should().Be(1);
        statusAfterMatch.TotalCount.Should().Be(1);

        writer.UnmatchReader(readerGuid);
        writer.MatchedReaderCount.Should().Be(0);
        var statusAfterUnmatch = writer.PublicationMatchedStatus;
        statusAfterUnmatch.CurrentCount.Should().Be(0);
        statusAfterUnmatch.TotalCount.Should().Be(1);

        writer.MatchReader(readerGuid, s.ReaderLocator);
        writer.MatchedReaderCount.Should().Be(1);
        var statusAfterRematch = writer.PublicationMatchedStatus;
        statusAfterRematch.CurrentCount.Should().Be(1);
        statusAfterRematch.TotalCount.Should().Be(2);
    }

    [Fact]
    public void PublicationMatchedStatus取得後にchange値がリセットされる()
    {
        var s = CreateSetup();
        var readerGuid = new Guid(s.ReaderPrefix, s.ReaderEntityId);
        using var writer = CreateWriter(s, out _);

        writer.MatchReader(readerGuid, s.ReaderLocator);
        _ = writer.PublicationMatchedStatus; // consume initial
        writer.MatchReader(new Guid(s.ReaderPrefix, new EntityId(0x0000_0003u, EntityKind.UserDefinedReaderNoKey)), s.ReaderLocator);

        var status = writer.PublicationMatchedStatus;
        status.CurrentCountChange.Should().Be(1);
        status.TotalCountChange.Should().Be(1);

        var statusAgain = writer.PublicationMatchedStatus;
        statusAgain.CurrentCountChange.Should().Be(0);
        statusAgain.TotalCountChange.Should().Be(0);
    }

    [Fact]
    public void 複数reliable_readerの最小ACKまでpurgeする()
    {
        var s = CreateSetup();
        var writerGuid = new Guid(s.WriterPrefix, s.WriterEntityId);
        var readerGuid1 = new Guid(s.ReaderPrefix, s.ReaderEntityId);
        var readerGuid2 = new Guid(GuidPrefix.Create(VendorId.ROSettaDDS, 0x11, 0x22, 0x03), s.ReaderEntityId);
        using var writer = CreateWriter(s, out var history);

        writer.MatchReader(readerGuid1, s.ReaderLocator);
        writer.MatchReader(readerGuid2, s.ReaderLocator);

        for (int i = 1; i <= 5; i++)
        {
            history.Add(ChangeKind.Alive, new byte[] { (byte)i }, Time.Now());
        }

        writer.GetReaderProxy(readerGuid1)!.ProcessAckNack(
            new SequenceNumberSet(new SequenceNumber(4L), 0, Array.Empty<uint>()));
        writer.GetReaderProxy(readerGuid2)!.ProcessAckNack(
            new SequenceNumberSet(new SequenceNumber(6L), 0, Array.Empty<uint>()));

        var ackPacket = BuildAckNackPacket(s.ReaderPrefix, s.ReaderEntityId, s.WriterEntityId,
            new SequenceNumberSet(new SequenceNumber(4L), 0, Array.Empty<uint>()));
        writer.ProcessPacket(ackPacket);

        history.Count.Should().Be(2, "minimum acked is SN=3, so SN<=3 should be purged");
    }

    [Fact]
    public void best_effort_readerはpurge判定から除外する()
    {
        var s = CreateSetup();
        var writerGuid = new Guid(s.WriterPrefix, s.WriterEntityId);
        var beReaderGuid = new Guid(s.ReaderPrefix, s.ReaderEntityId);
        using var writer = CreateWriter(s, out var history);

        writer.MatchReader(beReaderGuid, s.ReaderLocator, ReliabilityKind.BestEffort);

        for (int i = 1; i <= 3; i++)
        {
            history.Add(ChangeKind.Alive, new byte[] { (byte)i }, Time.Now());
        }

        var ackPacket = BuildAckNackPacket(s.ReaderPrefix, s.ReaderEntityId, s.WriterEntityId,
            new SequenceNumberSet(new SequenceNumber(4L), 0, Array.Empty<uint>()));
        writer.ProcessPacket(ackPacket);

        history.Count.Should().Be(3, "best-effort reader should not trigger purge");
    }

    [Fact]
    public void reliable_reader不在時はpurgeしない()
    {
        var s = CreateSetup();
        using var writer = CreateWriter(s, out var history);

        for (int i = 1; i <= 3; i++)
        {
            history.Add(ChangeKind.Alive, new byte[] { (byte)i }, Time.Now());
        }

        history.Count.Should().Be(3);
    }

    private static byte[] BuildAckNackPacket(
        GuidPrefix readerPrefix, EntityId readerEntityId, EntityId writerEntityId,
        SequenceNumberSet snSet)
    {
        var buffer = new byte[1500];
        var w = new RtpsMessageWriter(buffer, ProtocolVersion.V2_4, VendorId.ROSettaDDS, readerPrefix);
        w.WriteAckNack(new AckNackSubmessage(readerEntityId, writerEntityId, snSet, count: 1, final: false));
        var packet = new byte[w.BytesWritten];
        w.WrittenSpan.CopyTo(packet);
        return packet;
    }
}
