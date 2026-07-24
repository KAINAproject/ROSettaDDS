using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Dds;
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Discovery;
using ROSettaDDS.Msgs.Std;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Diagnostics;
using ROSettaDDS.Rcl.Naming;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rtps.Reader;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Tests.Rcl.Diagnostics;

public class TopicFrequencyMonitorTests
{
    private static Context CreateContext()
    {
        var options = new ContextOptions
        {
            Logger = NullLogger.Instance,
            LocalhostOnly = true,
            EnableAutomaticNetworkRecovery = false,
        };
        return new Context(options);
    }

    // ======== RawSubscription 基本動作 ========

    [Fact]
    public void RawSubscription_はraw_payload_callbackを受け取る()
    {
        var reader = new TestUserReader(new EntityId(1, EntityKind.UserDefinedReaderNoKey));
        byte[]? received = null;
        using var raw = new RawSubscription(
            "t", default, reader, (payload, _) => received = payload.ToArray(), autoStart: false);

        reader.SimulatePayload(new byte[] { 1, 2, 3 }, default);

        received.Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void RawSubscription_はDispose後にcallbackを発火しない()
    {
        var reader = new TestUserReader(new EntityId(2, EntityKind.UserDefinedReaderNoKey));
        int count = 0;
        var raw = new RawSubscription(
            "t", default, reader, (_, _) => Interlocked.Increment(ref count), autoStart: false);
        raw.Dispose();

        reader.SimulatePayload(ReadOnlyMemory<byte>.Empty, default);

        count.Should().Be(0);
    }

    [Fact]
    public void RawSubscription_は二重Disposeで例外を投げない()
    {
        var reader = new TestUserReader(new EntityId(3, EntityKind.UserDefinedReaderNoKey));
        var raw = new RawSubscription("t", default, reader, (_, _) => { }, autoStart: false);
        raw.Dispose();
        raw.Dispose();
    }

    // ======== 既定 QoS (BestEffort/Volatile) の match 検証 ========

    [Fact]
    public void 既定BestEffortVolatileのreaderは同じQoSのwriterとmatchする()
    {
        var r = new LocalReader(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Reader,
                TopicName = "rt/test",
                TypeName = "test::msg::dds_::Msg_",
                Reliability = ReliabilityQos.BestEffort,
                Durability = DurabilityQos.Volatile,
            }, null!);
        var w = new LocalWriter(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                TopicName = "rt/test",
                TypeName = "test::msg::dds_::Msg_",
                Reliability = ReliabilityQos.BestEffort,
                Durability = DurabilityQos.Volatile,
            }, null!);

        EndpointMatcher.EvaluateLocalLocal(r, w).IsCompatible.Should().BeTrue();
    }

    [Fact]
    public void 既定BestEffortVolatileのreaderはReliableTransientLocal_writerとmatchする()
    {
        var r = new LocalReader(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Reader,
                TopicName = "rt/test",
                TypeName = "test::msg::dds_::Msg_",
                Reliability = ReliabilityQos.BestEffort,
                Durability = DurabilityQos.Volatile,
            }, null!);
        var w = new LocalWriter(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                TopicName = "rt/test",
                TypeName = "test::msg::dds_::Msg_",
                Reliability = ReliabilityQos.Reliable,
                Durability = DurabilityQos.TransientLocal,
            }, null!);

        EndpointMatcher.EvaluateLocalLocal(r, w).IsCompatible.Should().BeTrue();
    }

    [Fact]
    public void 型名不一致でmatchしない()
    {
        var r = new LocalReader(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Reader,
                TopicName = "rt/test",
                TypeName = "type_a",
                Reliability = ReliabilityQos.BestEffort,
            }, null!);
        var w = new LocalWriter(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                TopicName = "rt/test",
                TypeName = "type_b",
                Reliability = ReliabilityQos.BestEffort,
            }, null!);

        EndpointMatcher.EvaluateLocalLocal(r, w).IsCompatible.Should().BeFalse();
    }

    [Fact]
    public void 既定QoSのreaderはremote_writerともmatchできる()
    {
        var r = new LocalReader(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Reader,
                TopicName = "rt/test",
                TypeName = "test::msg::dds_::Msg_",
                Reliability = ReliabilityQos.BestEffort,
                Durability = DurabilityQos.Volatile,
            }, null!);
        var remote = new RemoteEndpoint(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                TopicName = "rt/test",
                TypeName = "test::msg::dds_::Msg_",
                Reliability = ReliabilityQos.Reliable,
                Durability = DurabilityQos.Volatile,
            }, DateTime.UtcNow);

        EndpointMatcher.EvaluateLocalRemote(r, remote).IsCompatible.Should().BeTrue();
    }

    // ======== Node 統合: registration / SEDP / Dispose ========

    [Fact]
    public void CreateRawReader_はSEDPにreaderを広告する()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "raw_node");
        var initialCount = context.PublishedSubscriptionStateCount;

        using var raw = node.CreateRawReader(
            "rt/raw_test",
            "test::msg::dds_::Msg_",
            (_, _) => { },
            ReliabilityQos.BestEffort,
            DurabilityQos.Volatile);

        context.PublishedSubscriptionStateCount.Should().BeGreaterThan(initialCount);
    }

    [Fact]
    public void CreateRawReader_DisposeでSEDP_unregisterが発行される()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "sedp_node");
        var initialCount = context.PublishedSubscriptionStateCount;

        var raw = node.CreateRawReader(
            "rt/sedp_raw",
            "test::msg::dds_::Msg_",
            (_, _) => { },
            ReliabilityQos.BestEffort,
            DurabilityQos.Volatile);

        var afterCreate = context.PublishedSubscriptionStateCount;
        afterCreate.Should().BeGreaterThan(initialCount);

        raw.Dispose();

        var afterDispose = context.PublishedSubscriptionStateCount;
        afterDispose.Should().BeGreaterThan(afterCreate,
            "dispose must send SEDP unregister (increment published count)");
    }

    [Fact]
    public void Dispose後のNodeでCreateRawReaderはObjectDisposedException()
    {
        using var context = CreateContext();
        var node = new Node(context, "predisp_node");
        node.Dispose();
        var act = () => node.CreateRawReader(
            "rt/t", "t", (_, _) => { }, ReliabilityQos.BestEffort, DurabilityQos.Volatile);
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Context_Dispose後のRawSubscription_Disposeは安全()
    {
        var context = CreateContext();
        context.Start();
        var node = new Node(context, "ctx_first");
        var raw = node.CreateRawReader(
            "rt/ctx_first",
            "test::msg::dds_::Msg_",
            (_, _) => { },
            ReliabilityQos.BestEffort,
            DurabilityQos.Volatile);

        context.Dispose();

        raw.Dispose();
    }

    [Fact]
    public void SEDP広告前にDisposeしても安全()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "fast_disp");

        var raw = node.CreateRawReader(
            "rt/fast_disp",
            "test::msg::dds_::Msg_",
            (_, _) => { },
            ReliabilityQos.BestEffort,
            DurabilityQos.Volatile);

        raw.Dispose();
    }

    // ======== RawSubscription の callback 同時実行 ========

    [Fact]
    public void Dispose中のcallback同時実行で破損しない()
    {
        var reader = new TestUserReader(new EntityId(10, EntityKind.UserDefinedReaderNoKey));
        int callCount = 0;
        using var raw = new RawSubscription(
            "t", default, reader, (_, _) => Interlocked.Increment(ref callCount), autoStart: false);

        var barrier = new Barrier(3);
        var threads = new Thread[3];
        for (int i = 0; i < 3; i++)
        {
            threads[i] = new Thread(() =>
            {
                barrier.SignalAndWait();
                for (int j = 0; j < 100; j++)
                    reader.SimulatePayload(new byte[] { (byte)j }, default);
            });
        }

        foreach (var t in threads) t.Start();
        foreach (var t in threads) t.Join();

        callCount.Should().Be(300);
    }

    [Fact]
    public void Disposeとcallback同時実行で破損しない()
    {
        var reader = new TestUserReader(new EntityId(11, EntityKind.UserDefinedReaderNoKey));
        var raw = new RawSubscription(
            "t", default, reader, (_, _) => Thread.SpinWait(100), autoStart: false);

        var callbackThread = new Thread(() =>
        {
            for (int i = 0; i < 50; i++)
                reader.SimulatePayload(new byte[] { (byte)i }, default);
        });
        callbackThread.Start();

        Thread.SpinWait(50);
        raw.Dispose();

        callbackThread.Join();
    }

    // ======== TopicFrequencyOptions ========

    [Fact]
    public void TopicFrequencyOptions_既定値()
    {
        var opts = new TopicFrequencyOptions();
        opts.WindowSize.Should().Be(TopicFrequencyOptions.DefaultWindowSize);
        opts.Reliability.Should().Be(ReliabilityQos.BestEffort);
        opts.Durability.Should().Be(DurabilityQos.Volatile);
    }

    [Fact]
    public void TopicFrequencyOptions_WindowSize下限2未満でArgumentException()
    {
        var act = () => new TopicFrequencyOptions(1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TopicFrequencyOptions_WindowSize上限超過でArgumentException()
    {
        var act = () => new TopicFrequencyOptions(TopicFrequencyOptions.MaxWindowSize + 1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TopicFrequencyOptions_Defaultは常に同じインスタンス()
    {
        TopicFrequencyOptions.Default.Should().BeSameAs(TopicFrequencyOptions.Default);
    }

    [Fact]
    public void TopicFrequencyOptions_カスタム値()
    {
        var opts = new TopicFrequencyOptions(
            100, ReliabilityQos.Reliable, DurabilityQos.TransientLocal);
        opts.WindowSize.Should().Be(100);
        opts.Reliability.Should().Be(ReliabilityQos.Reliable);
        opts.Durability.Should().Be(DurabilityQos.TransientLocal);
    }

    // ======== IClock / ring buffer ========

    [Fact]
    public void 記録したtimestampが統計に反映される()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        for (int i = 0; i < 5; i++)
            monitor.Record(clock.Advance(100_000));

        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(5);
        stats.HasData.Should().BeTrue();
    }

    [Fact]
    public void 未記録でSampleCount0_HasDataFalse()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(0);
        stats.HasData.Should().BeFalse();
    }

    [Fact]
    public void 単一記録でHasDataFalse()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        monitor.Record(clock.Advance(100_000));
        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(1);
        stats.HasData.Should().BeFalse();
    }

    [Fact]
    public void 同timestampでHasDataFalse()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        long ts = clock.Advance(100_000);
        monitor.Record(ts);
        monitor.Record(ts); // same timestamp

        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(2);
        stats.HasData.Should().BeFalse("last timestamp is not greater than first");
    }

    [Fact]
    public void RateHzが正しく計算される()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        // 500ms intervals = 5_000_000 ticks (1 tick = 100ns)
        // 4 samples → 3 intervals → 1500ms → 2Hz
        for (int i = 0; i < 4; i++)
            monitor.Record(clock.Advance(5_000_000));

        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(4);
        stats.RateHz.Should().BeApproximately(2.0, 0.001);
    }

    [Fact]
    public void リングバッファがwrapして再計算される()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 3);

        // 200ms intervals = 2_000_000 ticks
        // Record 5 timestamps → buffer holds last 3 (wrap)
        for (int i = 0; i < 5; i++)
            monitor.Record(clock.Advance(2_000_000));

        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(5);
        // WindowSize=3: last 3 timestamps → 2 intervals → 400ms → 5Hz
        stats.RateHz.Should().BeApproximately(5.0, 0.001);
    }

    [Fact]
    public void MinMaxMeanIntervalが正しい()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        // 1 tick = 100ns in MockClock
        // 2_000_000 ticks = 200ms, 500_000 = 50ms, 3_000_000 = 300ms
        monitor.Record(clock.Advance(1_000_000));  // t0 at +100ms
        monitor.Record(clock.Advance(2_000_000));  // +200ms → interval 200ms
        monitor.Record(clock.Advance(500_000));    // +50ms  → interval 50ms
        monitor.Record(clock.Advance(3_000_000));  // +300ms → interval 300ms

        var stats = monitor.ComputeStatistics();
        stats.MinInterval.Should().Be(TimeSpan.FromMilliseconds(50));
        stats.MaxInterval.Should().Be(TimeSpan.FromMilliseconds(300));
        // mean = (200+50+300)/3 ≈ 183.33ms
        stats.MeanInterval.TotalMilliseconds.Should().BeApproximately(183.333, 0.01);
    }

    [Fact]
    public void StdDevが正しい()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        // All intervals = 100ms = 1_000_000 ticks
        monitor.Record(clock.Advance(1_000_000));  // t0
        monitor.Record(clock.Advance(1_000_000));  // +100ms
        monitor.Record(clock.Advance(1_000_000));  // +100ms
        monitor.Record(clock.Advance(1_000_000));  // +100ms

        var stats = monitor.ComputeStatistics();
        stats.StdDevInterval.TotalMilliseconds.Should().BeApproximately(0, 0.001);
    }

    [Fact]
    public void Durationが最初から最後までの経過時間()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        monitor.Record(clock.Advance(1_000_000));    // t0 at 100ms
        monitor.Record(clock.Advance(2_000_000));    // t1 at 300ms (+200ms)
        monitor.Record(clock.Advance(3_000_000));    // t2 at 600ms (+300ms)

        var stats = monitor.ComputeStatistics();
        // duration = 600ms - 100ms = 500ms
        stats.Duration.Should().Be(TimeSpan.FromMilliseconds(500));
    }

    // ======== CreateFrequencyMonitor ========

    [Fact]
    public void CreateFrequencyMonitor_がインスタンスを返す()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "fm_node");
        using var pub = node.CreatePublisher<StringMessage>(
            "fm_topic", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var diag = node.CreateTopicDiagnostics();

        using var monitor = diag.CreateFrequencyMonitor("/fm_topic");
        monitor.Should().NotBeNull();
    }

    [Fact]
    public void CreateFrequencyMonitor_未発見topicでTopicNotFoundException()
    {
        using var context = CreateContext();
        using var node = new Node(context, "nf_node");
        using var diag = node.CreateTopicDiagnostics();

        var act = () => diag.CreateFrequencyMonitor("/nonexistent");
        act.Should().Throw<TopicNotFoundException>();
    }

    [Fact]
    public void CreateFrequencyMonitor_空DDS型でAmbiguousTopicTypeException()
    {
        using var context = CreateContext();
        using var node = new Node(context, "empty_type_node");
        using var diag = node.CreateTopicDiagnostics();

        var prefix = Prefix(40);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/empty_type", ""), DateTime.UtcNow);

        var act = () => diag.CreateFrequencyMonitor("/empty_type");
        act.Should().Throw<AmbiguousTopicTypeException>();
    }

    [Fact]
    public void CreateFrequencyMonitor_複数DDS型でAmbiguousTopicTypeException()
    {
        using var context = CreateContext();
        using var node = new Node(context, "multi_type_node");
        using var diag = node.CreateTopicDiagnostics();

        var prefix = Prefix(41);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/multi_type",
                "type_a::dds_::A_"), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Writer, 0x11, "rt/multi_type",
                "type_b::dds_::B_"), DateTime.UtcNow);

        var act = () => diag.CreateFrequencyMonitor("/multi_type");
        act.Should().Throw<AmbiguousTopicTypeException>();
    }

    // ======== WaitForMatchedAsync ========

    [Fact]
    public async Task WaitForMatchedAsync_がwriterとマッチするとtrue()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "match_node");
        using var pub = node.CreatePublisher<StringMessage>(
            "match_topic", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var diag = node.CreateTopicDiagnostics();
        using var monitor = diag.CreateFrequencyMonitor("/match_topic");

        var result = await monitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(5));
        result.Should().BeTrue();
    }

    [Fact]
    public async Task WaitForMatchedAsync_タイムアウトでfalse()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "timeout_node");

        // Start remote reader on the topic; no remote writers on "rt/timeout_topic"
        var prefix = Prefix(42);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Reader, 0x10, "rt/timeout_topic"), DateTime.UtcNow);

        using var diag = node.CreateTopicDiagnostics();
        using var monitor = diag.CreateFrequencyMonitor("/timeout_topic");

        var result = await monitor.WaitForMatchedAsync(1, TimeSpan.FromMilliseconds(100));
        result.Should().BeFalse();
    }

    [Fact]
    public async Task WaitForMatchedAsync_キャンセルでOperationCanceledException()
    {
        using var context = CreateContext();
        using var node = new Node(context, "cancel_node");
        using var diag = node.CreateTopicDiagnostics();

        var prefix = Prefix(43);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/cancel_topic"), DateTime.UtcNow);
        using var monitor = diag.CreateFrequencyMonitor("/cancel_topic");

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => monitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(5), cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    // ======== Dispose / lifecycle ========

    [Fact]
    public void Dispose後のComputeStatisticsは例外を投げない()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        monitor.Dispose();

        var act = () => monitor.ComputeStatistics();
        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose後のRecordは無視される()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        monitor.Dispose();

        monitor.Record(clock.Advance(100_000));
        var stats = monitor.ComputeStatistics();
        stats.SampleCount.Should().Be(0);
    }

    [Fact]
    public void 二重Disposeで例外を投げない()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        monitor.Dispose();
        monitor.Dispose();
    }

    [Fact]
    public void CreateFrequencyMonitorのDisposeでSEDP_unregisterが発行される()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "sedp_fm");
        using var pub = node.CreatePublisher<StringMessage>(
            "sedp_fm_topic", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        using var diag = node.CreateTopicDiagnostics();
        var initialCount = context.PublishedSubscriptionStateCount;

        var monitor = diag.CreateFrequencyMonitor("/sedp_fm_topic");
        var afterCreate = context.PublishedSubscriptionStateCount;
        afterCreate.Should().BeGreaterThan(initialCount);

        monitor.Dispose();
        var afterDispose = context.PublishedSubscriptionStateCount;
        afterDispose.Should().BeGreaterThan(afterCreate,
            "dispose must send SEDP unregister");
    }

    // ======== Test helper ========

    private static GuidPrefix Prefix(byte id)
        => GuidPrefix.Create(VendorId.ROSettaDDS, id, (uint)(0x1000 + id), (ushort)(0x2000 + id));

    private static ParticipantData Participant(GuidPrefix prefix)
        => new()
        {
            Guid = new Guid(prefix, EntityId.Participant),
            LeaseDuration = Duration.Infinite,
        };

    private static DiscoveredEndpointData Endpoint(
        GuidPrefix prefix,
        EndpointKind kind,
        uint entityKey,
        string topic,
        string type = "std_msgs::msg::dds_::String_")
        => new()
        {
            Kind = kind,
            EndpointGuid = new Guid(
                prefix,
                new EntityId(
                    entityKey,
                    kind == EndpointKind.Writer
                        ? EntityKind.UserDefinedWriterNoKey
                        : EntityKind.UserDefinedReaderNoKey)),
            ParticipantGuid = new Guid(prefix, EntityId.Participant),
            TopicName = topic,
            TypeName = type,
        };

    private static TopicFrequencyMonitor CreateMonitorWithClock(MockClock clock, int windowSize)
    {
        var opts = new TopicFrequencyOptions(windowSize);
        return new TopicFrequencyMonitor(opts, clock);
    }

    private sealed class MockClock : IClock
    {
        private long _now;

        public MockClock(long initialTicks = 0)
        {
            _now = initialTicks;
        }

        public long GetTimestamp() => _now;

        public TimeSpan GetElapsedTime(long startingTimestamp, long endingTimestamp)
        {
            // Simulate Stopwatch ticks: 1 tick = 100ns for this mock
            return TimeSpan.FromTicks(endingTimestamp - startingTimestamp);
        }

        /// <summary>Advance clock by given ticks and return new timestamp.</summary>
        public long Advance(long ticks)
        {
            _now += ticks;
            return _now;
        }
    }

    private sealed class TestUserReader : IUserReader
    {
        public TestUserReader(EntityId readerEntityId)
        {
            ReaderEntityId = readerEntityId;
            Guid = new Guid(GuidPrefix.Unknown, readerEntityId);
        }

        public EntityId ReaderEntityId { get; }
        public Guid Guid { get; }
        public IRtpsSubmessageHandler Handler =>
            throw new NotSupportedException("TestUserReader does not support Handler");
        public event Action<ReadOnlyMemory<byte>, GuidPrefix>? PayloadReceived;
        public int MatchedWriterCount => 0;
        public SubscriptionMatchedStatus SubscriptionMatchedStatus => default;
        public RtpsReaderDiagnostics Diagnostics =>
            throw new NotSupportedException("TestUserReader does not support Diagnostics");

        public void MatchWriter(Guid writerGuid, Locator? unicastReplyLocator) { }
        public void UnmatchWriter(Guid writerGuid) { }
        public void Start() { }
        public void Stop() { }
        public void Dispose() { }

        public void SimulatePayload(ReadOnlyMemory<byte> payload, GuidPrefix sourcePrefix)
            => PayloadReceived?.Invoke(payload, sourcePrefix);
    }
}
