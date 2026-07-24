using System.Reflection;
using ROSettaDDS.Cdr;
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
        var exceptions = new List<Exception>();
        var threads = new Thread[3];
        for (int i = 0; i < 3; i++)
        {
            threads[i] = new Thread(() =>
            {
                try
                {
                    barrier.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();
                    for (int j = 0; j < 100; j++)
                        reader.SimulatePayload(new byte[] { (byte)j }, default);
                }
                catch (Exception ex)
                {
                    lock (exceptions) { exceptions.Add(ex); }
                }
            });
        }

        foreach (var t in threads) t.Start();
        try
        {
            foreach (var t in threads)
                t.Join(TimeSpan.FromSeconds(5)).Should().BeTrue("worker thread must complete within 5s");
        }
        finally
        {
            barrier.Dispose();
        }

        if (exceptions.Count > 0)
            throw new AggregateException("Worker threads threw exceptions", exceptions);

        callCount.Should().Be(300);
    }

    [Fact]
    public void Disposeとcallback同時実行で破損しない()
    {
        var reader = new TestUserReader(new EntityId(11, EntityKind.UserDefinedReaderNoKey));
        using var callbackEntered = new Barrier(2);
        using var continueBarrier = new Barrier(2);
        Exception? workerException = null;

        var raw = new RawSubscription(
            "t", default, reader, (_, _) =>
            {
                callbackEntered.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();
                continueBarrier.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            }, autoStart: false);

        var callbackThread = new Thread(() =>
        {
            try
            {
                reader.SimulatePayload(new byte[] { 1 }, default);
            }
            catch (Exception ex)
            {
                workerException = ex;
            }
        });
        callbackThread.Start();

        // Wait for callback to enter, then dispose while callback is held
        callbackEntered.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();
        raw.Dispose();

        // Release callback
        continueBarrier.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();

        callbackThread.Join(TimeSpan.FromSeconds(5)).Should().BeTrue("worker must complete within 5s");

        if (workerException is not null)
            throw new AggregateException("Worker thread threw exception", workerException);
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
        var act = () => new TopicFrequencyOptions { WindowSize = 1 };
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TopicFrequencyOptions_WindowSize上限超過でArgumentException()
    {
        using var context = CreateContext();
        using var node = new Node(context, "empty_type_node");
        var act = () => new TopicFrequencyOptions { WindowSize = 1_000_001 };
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
        var opts = new TopicFrequencyOptions
        {
            WindowSize = 100,
            Reliability = ReliabilityQos.Reliable,
            Durability = DurabilityQos.TransientLocal,
        };
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

        var stats = monitor.GetStatistics();
        stats.SampleCount.Should().Be(5);
        stats.HasData.Should().BeTrue();
    }

    [Fact]
    public void 未記録でSampleCount0_HasDataFalse()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var stats = monitor.GetStatistics();
        stats.SampleCount.Should().Be(0);
        stats.HasData.Should().BeFalse();
    }

    [Fact]
    public void 単一記録でHasDataFalse()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        monitor.Record(clock.Advance(100_000));
        var stats = monitor.GetStatistics();
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

        var stats = monitor.GetStatistics();
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

        var stats = monitor.GetStatistics();
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

        var stats = monitor.GetStatistics();
        // ring buffer holds 3 entries = WindowSize
        stats.SampleCount.Should().Be(3);
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

        var stats = monitor.GetStatistics();
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

        var stats = monitor.GetStatistics();
        stats.StandardDeviation.TotalMilliseconds.Should().BeApproximately(0, 0.001);
    }

    [Fact]
    public void Durationが最初から最後までの経過時間()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        monitor.Record(clock.Advance(1_000_000));    // t0 at 100ms
        monitor.Record(clock.Advance(2_000_000));    // t1 at 300ms (+200ms)
        monitor.Record(clock.Advance(3_000_000));    // t2 at 600ms (+300ms)

        var stats = monitor.GetStatistics();
        // duration = 600ms - 100ms = 500ms
        stats.WindowDuration.Should().Be(TimeSpan.FromMilliseconds(500));
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
    public void Dispose後のGetStatisticsはObjectDisposedException()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        monitor.Dispose();

        var act = () => monitor.GetStatistics();
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose後のRecordはObjectDisposedException()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        monitor.Dispose();

        var act = () => monitor.Record(clock.Advance(100_000));
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task Dispose後のWaitForMatchedAsyncはObjectDisposedException()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        monitor.Dispose();

        var act = () => monitor.WaitForMatchedAsync(1, TimeSpan.FromMilliseconds(100));
        await act.Should().ThrowAsync<ObjectDisposedException>();
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
    public void Disposeの競合をBarrierで同時実行()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var barrier = new Barrier(2);
        Exception? ex1 = null, ex2 = null;
        var t1 = new Thread(() =>
        {
            try
            {
                barrier.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();
                monitor.Dispose();
            }
            catch (Exception ex) { ex1 = ex; }
        });
        var t2 = new Thread(() =>
        {
            try
            {
                barrier.SignalAndWait(TimeSpan.FromSeconds(5)).Should().BeTrue();
                monitor.Dispose();
            }
            catch (Exception ex) { ex2 = ex; }
        });

        t1.Start();
        t2.Start();
        try
        {
            t1.Join(TimeSpan.FromSeconds(5)).Should().BeTrue();
            t2.Join(TimeSpan.FromSeconds(5)).Should().BeTrue();
        }
        finally
        {
            barrier.Dispose();
        }

        if (ex1 is not null) throw new AggregateException("Thread1 threw", ex1);
        if (ex2 is not null) throw new AggregateException("Thread2 threw", ex2);
    }

    [Fact]
    public async Task Dispose中のWaitForMatchedAsyncはキャンセルされる()
    {
        var clock = new MockClock();
        var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var waitTask = monitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(10));

        monitor.Dispose();

        var act = async () => await waitTask;
        // dispose cancels the wait via CTS → ObjectDisposedException
        await act.Should().ThrowAsync<ObjectDisposedException>();
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

    // ======== Spec Review: SampleCount / ring buffer ========

    [Fact]
    public void リングバッファ未満の記録でSampleCountは記録数と一致()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);
        for (int i = 0; i < 7; i++)
            monitor.Record(clock.Advance(100_000));

        var stats = monitor.GetStatistics();
        stats.SampleCount.Should().Be(7);
    }

    [Fact]
    public void リングバッファ超過後にSampleCountはWindowSizeと一致()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 4);
        // 6 records → ring buffer overwrites → holds last 4
        for (int i = 0; i < 6; i++)
            monitor.Record(clock.Advance(100_000));

        var stats = monitor.GetStatistics();
        stats.SampleCount.Should().Be(4);
    }

    // ======== Spec Review: stddev with non-uniform intervals ========

    [Fact]
    public void 非均一intervalで母標準偏差が正しい()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        // intervals: 100ms, 200ms, 300ms (1_000_000, 2_000_000, 3_000_000 ticks)
        // mean = 200ms
        // variance = ((100-200)^2 + (200-200)^2 + (300-200)^2) / 3
        //          = (10000 + 0 + 10000) / 3 = 6666.67
        // stddev = sqrt(6666.67) = 81.65ms
        monitor.Record(clock.Advance(1_000_000));  // t0
        monitor.Record(clock.Advance(2_000_000));  // +200ms
        monitor.Record(clock.Advance(3_000_000));  // +300ms
        monitor.Record(clock.Advance(4_000_000));  // +400ms

        var stats = monitor.GetStatistics();
        stats.StandardDeviation.TotalMilliseconds.Should().BeApproximately(81.65, 0.01);
    }

    // ======== Spec Review: WaitForMatchedAsync ========

    [Fact]
    public async Task WaitForMatchedAsync_minCount0で即true()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var result = await monitor.WaitForMatchedAsync(0, TimeSpan.FromSeconds(5));
        result.Should().BeTrue();
    }

    [Fact]
    public async Task WaitForMatchedAsync_負のタイムアウトでArgumentOutOfRangeException()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var act = () => monitor.WaitForMatchedAsync(1, TimeSpan.FromMilliseconds(-2));
        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task WaitForMatchedAsync_Infiniteタイムアウトは許可される()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        // Should not throw - Infinite is allowed
        var act = () => monitor.WaitForMatchedAsync(1, System.Threading.Timeout.InfiniteTimeSpan);
        // It'll wait forever, so we need to dispose to stop it
        var task = act();
        monitor.Dispose();
        var ex = await Record.ExceptionAsync(() => task);
        // Expected to throw ObjectDisposedException, not ArgumentOutOfRangeException
        ex.Should().BeOfType<ObjectDisposedException>();
    }

    [Fact]
    public async Task WaitForMatchedAsync_タイムアウト0で即false()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var result = await monitor.WaitForMatchedAsync(1, TimeSpan.Zero);
        result.Should().BeFalse();
    }

    // ======== Spec Review: lifecycle ========

    [Fact]
    public void TopicDiagnostics_Disposeで全monitorがDisposeされる()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "diag_disp");
        using var pub = node.CreatePublisher<StringMessage>(
            "diag_disp_topic", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var diag = node.CreateTopicDiagnostics();
        var monitor = diag.CreateFrequencyMonitor("/diag_disp_topic");

        diag.Dispose();

        // monitor should be disposed → Record throws
        var act = () => monitor.Record(0);
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Node_Disposeで全diagnosticsがDisposeされる()
    {
        using var context = CreateContext();
        context.Start();
        var node = new Node(context, "node_disp_diag");
        using var pub = node.CreatePublisher<StringMessage>(
            "node_disp_top", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var diag = node.CreateTopicDiagnostics();
        var monitor = diag.CreateFrequencyMonitor("/node_disp_top");

        node.Dispose();

        // diag and monitor should be disposed
        var act = () => monitor.Record(0);
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Context_Disposeで全monitor連鎖Disposeされる()
    {
        var context = CreateContext();
        context.Start();
        var node = new Node(context, "ctx_disp");
        using var pub = node.CreatePublisher<StringMessage>(
            "ctx_disp_top", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var diag = node.CreateTopicDiagnostics();
        var monitor = diag.CreateFrequencyMonitor("/ctx_disp_top");

        // simulate: nondisposed diagnostics on node, context disposes
        // This would call Node.Dispose → TopicDiagnostics.Dispose → monitor.Dispose
        context.Dispose();

        var act = () => monitor.Record(0);
        act.Should().Throw<ObjectDisposedException>();
    }

    // ======== Spec Review: Public API surface reflection ========

    [Fact]
    public void Diagnostics名前空間の公開型一覧()
    {
        var types = typeof(TopicFrequencyMonitor).Assembly.GetTypes()
            .Where(t => t.Namespace == typeof(TopicFrequencyMonitor).Namespace && t.IsPublic)
            .Select(t => t.Name)
            .OrderBy(n => n)
            .ToArray();
        types.Should().BeEquivalentTo(new[]
        {
            "AmbiguousTopicTypeException",
            "TopicDiagnostics",
            "TopicEndpointInfo",
            "TopicFrequencyMonitor",
            "TopicFrequencyOptions",
            "TopicFrequencyStatistics",
            "TopicInfo",
            "TopicNotFoundException",
        });
    }

    [Fact]
    public void TopicEndpointInfo_公開API()
    {
        var t = typeof(TopicEndpointInfo);

        t.GetConstructors(BindingFlags.Public | BindingFlags.Instance).Should().BeEmpty();

        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.Name).ToArray();
        var expected = new (string name, Type type, bool canRead, bool canWrite)[]
        {
            ("DdsTopicName", typeof(string), true, false),
            ("DdsTypeName", typeof(string), true, false),
            ("Durability", typeof(DurabilityQos), true, false),
            ("EndpointGuid", typeof(Guid), true, false),
            ("IsLocal", typeof(bool), true, false),
            ("Kind", typeof(EndpointKind), true, false),
            ("Reliability", typeof(ReliabilityQos), true, false),
            ("RosTypeName", typeof(string), true, false),
            ("TopicName", typeof(string), true, false),
        };
        props.Select(p => (p.Name, p.PropertyType, p.CanRead, p.CanWrite))
            .Should().BeEquivalentTo(expected);

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName).ToArray();
        methods.Should().BeEmpty();
    }

    [Fact]
    public void TopicInfo_公開API()
    {
        var t = typeof(TopicInfo);

        t.GetConstructors(BindingFlags.Public | BindingFlags.Instance).Should().BeEmpty();

        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.Name).ToArray();
        var expected = new (string name, Type type, bool canRead, bool canWrite)[]
        {
            ("Endpoints", typeof(IReadOnlyList<TopicEndpointInfo>), true, false),
            ("PublisherCount", typeof(int), true, false),
            ("RosTypeNames", typeof(IReadOnlyList<string>), true, false),
            ("SubscriberCount", typeof(int), true, false),
            ("TopicName", typeof(string), true, false),
        };
        props.Select(p => (p.Name, p.PropertyType, p.CanRead, p.CanWrite))
            .Should().BeEquivalentTo(expected);

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName).ToArray();
        methods.Should().BeEmpty();
    }

    [Fact]
    public void TopicFrequencyStatistics_公開API()
    {
        var t = typeof(TopicFrequencyStatistics);

        t.GetConstructors(BindingFlags.Public | BindingFlags.Instance).Should().BeEmpty();

        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.Name).ToArray();
        var expected = new (string name, Type type, bool canRead, bool canWrite)[]
        {
            ("HasData", typeof(bool), true, false),
            ("MaxInterval", typeof(TimeSpan), true, false),
            ("MeanInterval", typeof(TimeSpan), true, false),
            ("MinInterval", typeof(TimeSpan), true, false),
            ("RateHz", typeof(double), true, false),
            ("SampleCount", typeof(int), true, false),
            ("StandardDeviation", typeof(TimeSpan), true, false),
            ("WindowDuration", typeof(TimeSpan), true, false),
        };
        props.Select(p => (p.Name, p.PropertyType, p.CanRead, p.CanWrite))
            .Should().BeEquivalentTo(expected);

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName).ToArray();
        methods.Should().BeEmpty();
    }

    [Fact]
    public void TopicFrequencyOptions_公開API()
    {
        var t = typeof(TopicFrequencyOptions);

        var ctors = t.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        ctors.Should().ContainSingle();
        ctors[0].GetParameters().Should().BeEmpty();

        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.Name).ToArray();
        var expectedProps = new (string name, Type type, bool canRead, bool isInitOnly)[]
        {
            ("Durability", typeof(DurabilityQos), true, true),
            ("Reliability", typeof(ReliabilityQos), true, true),
            ("WindowSize", typeof(int), true, true),
        };
        foreach (var (name, type, canRead, isInitOnly) in expectedProps)
        {
            var prop = props.Should().ContainSingle(p => p.Name == name).Subject;
            prop.PropertyType.Should().Be(type);
            prop.CanRead.Should().Be(canRead);
            prop.CanWrite.Should().BeTrue();
        }
        props.Length.Should().Be(expectedProps.Length);

        // Verify init-only setters via IsExternalInit modreq
        var isExternalInitType = Type.GetType("System.Runtime.CompilerServices.IsExternalInit");
        isExternalInitType.Should().NotBeNull();
        foreach (var prop in props)
        {
            var setMethod = prop.SetMethod;
            setMethod.Should().NotBeNull("property " + prop.Name + " must have a setter");
            var modifiers = setMethod!.ReturnParameter.GetRequiredCustomModifiers();
            modifiers.Should().Contain(isExternalInitType!,
                "property " + prop.Name + " setter must be init-only");
        }

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName).ToArray();
        methods.Should().BeEmpty();
    }

    [Fact]
    public void TopicFrequencyMonitor_公開API()
    {
        var t = typeof(TopicFrequencyMonitor);

        t.GetConstructors(BindingFlags.Public | BindingFlags.Instance).Should().BeEmpty();

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName)
            .GroupBy(m => m.Name)
            .OrderBy(g => g.Key)
            .ToArray();

        methods.Length.Should().Be(3);
        methods[0].Key.Should().Be("Dispose");
        methods[0].Count().Should().Be(1);
        methods[0].Single().ReturnType.Should().Be(typeof(void));
        methods[0].Single().GetParameters().Should().BeEmpty();

        methods[1].Key.Should().Be("GetStatistics");
        methods[1].Count().Should().Be(1);
        methods[1].Single().ReturnType.Should().Be(typeof(TopicFrequencyStatistics));
        methods[1].Single().GetParameters().Should().BeEmpty();

        methods[2].Key.Should().Be("WaitForMatchedAsync");
        methods[2].Count().Should().Be(1);
        methods[2].Single().ReturnType.Should().Be(typeof(Task<bool>));
        var wfmaParams = methods[2].Single().GetParameters();
        wfmaParams.Length.Should().Be(3);
        wfmaParams[0].Name.Should().Be("minCount");
        wfmaParams[0].ParameterType.Should().Be(typeof(int));
        wfmaParams[1].Name.Should().Be("timeout");
        wfmaParams[1].ParameterType.Should().Be(typeof(TimeSpan));
        wfmaParams[2].Name.Should().Be("cancellationToken");
        wfmaParams[2].ParameterType.Should().Be(typeof(CancellationToken));
        wfmaParams[2].IsOptional.Should().BeTrue();
        wfmaParams[2].HasDefaultValue.Should().BeTrue();
        wfmaParams[2].DefaultValue.Should().BeNull();
    }

    [Fact]
    public void TopicDiagnostics_公開API()
    {
        var t = typeof(TopicDiagnostics);

        t.GetConstructors(BindingFlags.Public | BindingFlags.Instance).Should().BeEmpty();

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName)
            .GroupBy(m => m.Name)
            .OrderBy(g => g.Key)
            .ToArray();

        methods.Length.Should().Be(4);
        methods[0].Key.Should().Be("CreateFrequencyMonitor");
        methods[0].Count().Should().Be(1);
        methods[0].Single().ReturnType.Should().Be(typeof(TopicFrequencyMonitor));
        var cfmParams = methods[0].Single().GetParameters();
        cfmParams.Length.Should().Be(2);
        cfmParams[0].Name.Should().Be("topicName");
        cfmParams[0].ParameterType.Should().Be(typeof(string));
        cfmParams[1].Name.Should().Be("options");
        cfmParams[1].ParameterType.Should().Be(typeof(TopicFrequencyOptions));
        cfmParams[1].IsOptional.Should().BeTrue();
        cfmParams[1].HasDefaultValue.Should().BeTrue();
        cfmParams[1].DefaultValue.Should().BeNull();

        methods[1].Key.Should().Be("Dispose");
        methods[1].Count().Should().Be(1);
        methods[1].Single().ReturnType.Should().Be(typeof(void));
        methods[1].Single().GetParameters().Should().BeEmpty();

        methods[2].Key.Should().Be("GetTopicInfo");
        methods[2].Count().Should().Be(1);
        // ReturnType should be TopicInfo (non-nullable type ref) adjusted for nullable
        methods[2].Single().ReturnType.Should().Be(typeof(TopicInfo));
        // Check nullable via NullableAttribute on return parameter (Nullable(2) = nullable)
        var returnParam = methods[2].Single().ReturnParameter;
        var nullableAttr = returnParam.GetCustomAttributes(false)
            .FirstOrDefault(a => a.GetType().Name == "NullableAttribute");
        nullableAttr.Should().NotBeNull("GetTopicInfo must return nullable TopicInfo?");
        var gtiParams = methods[2].Single().GetParameters();
        gtiParams.Length.Should().Be(1);
        gtiParams[0].Name.Should().Be("topicName");
        gtiParams[0].ParameterType.Should().Be(typeof(string));

        methods[3].Key.Should().Be("GetTopics");
        methods[3].Count().Should().Be(1);
        methods[3].Single().ReturnType.Should().Be(typeof(IReadOnlyList<TopicInfo>));
        methods[3].Single().GetParameters().Should().BeEmpty();
    }

    [Fact]
    public void TopicNotFoundException_公開API()
    {
        var t = typeof(TopicNotFoundException);

        var ctors = t.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        ctors.Should().ContainSingle();
        var ctorParams = ctors[0].GetParameters();
        ctorParams.Length.Should().Be(1);
        ctorParams[0].Name.Should().Be("topicName");
        ctorParams[0].ParameterType.Should().Be(typeof(string));

        t.BaseType.Should().Be(typeof(InvalidOperationException));

        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name).OrderBy(n => n).ToArray();
        props.Should().BeEquivalentTo(new[] { "Data", "HelpLink", "HResult", "InnerException", "Message", "Source", "StackTrace", "TargetSite" });
    }

    [Fact]
    public void AmbiguousTopicTypeException_公開API()
    {
        var t = typeof(AmbiguousTopicTypeException);

        var ctors = t.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        ctors.Should().ContainSingle();
        var ctorParams = ctors[0].GetParameters();
        ctorParams.Length.Should().Be(1);
        ctorParams[0].Name.Should().Be("topicName");
        ctorParams[0].ParameterType.Should().Be(typeof(string));

        t.BaseType.Should().Be(typeof(InvalidOperationException));
    }

    [Fact]
    public void 内部型は公開されない()
    {
        typeof(IClock).IsVisible.Should().BeFalse();
        typeof(SystemClock).IsVisible.Should().BeFalse();
        typeof(RawSubscription).IsVisible.Should().BeFalse();
    }

    [Fact]
    public void Node_CreateTopicDiagnostics_公開シグネチャ()
    {
        var method = typeof(Node).GetMethods(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .First(m => m.Name == "CreateTopicDiagnostics");
        method.ReturnType.Should().Be(typeof(TopicDiagnostics));
        method.GetParameters().Should().BeEmpty();
    }

    [Fact]
    public void Node_公開API()
    {
        var t = typeof(Node);

        var ctors = t.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        ctors.Should().ContainSingle();
        var ctorParams = ctors[0].GetParameters();
        ctorParams.Length.Should().Be(3);
        ctorParams[0].Name.Should().Be("context");
        ctorParams[0].ParameterType.Should().Be(typeof(Context));
        ctorParams[1].Name.Should().Be("name");
        ctorParams[1].ParameterType.Should().Be(typeof(string));
        ctorParams[2].Name.Should().Be("options");
        ctorParams[2].ParameterType.Should().Be(typeof(NodeOptions));
        ctorParams[2].IsOptional.Should().BeTrue();
        ctorParams[2].HasDefaultValue.Should().BeTrue();
        ctorParams[2].DefaultValue.Should().BeNull();

        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.Name).ToArray();
        var expectedProps = new (string Name, Type Type, bool CanRead, bool CanWrite)[]
        {
            ("Context", typeof(Context), true, false),
            ("Name", typeof(string), true, false),
            ("Options", typeof(NodeOptions), true, false),
        };
        props.Select(p => (p.Name, p.PropertyType, p.CanRead, p.CanWrite))
            .Should().BeEquivalentTo(expectedProps);

        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName)
            .OrderBy(m => m.Name)
            .ThenBy(m => m.GetParameters().Length)
            .ToArray();

        methods.Length.Should().Be(7);

        // --- CreatePublisher (3-param overload) ---
        var pub3 = methods[0];
        pub3.Name.Should().Be("CreatePublisher");
        pub3.IsGenericMethod.Should().BeTrue();
        pub3.GetGenericArguments().Length.Should().Be(1);
        pub3.ReturnType.GetGenericTypeDefinition().Should().Be(typeof(Publisher<>));
        var pub3Params = pub3.GetParameters();
        pub3Params.Length.Should().Be(3);
        pub3Params[0].Name.Should().Be("topicName");
        pub3Params[0].ParameterType.Should().Be(typeof(string));
        pub3Params[0].IsOptional.Should().BeFalse();
        pub3Params[1].Name.Should().Be("serializer");
        pub3Params[1].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(ICdrSerializer<>));
        pub3Params[1].IsOptional.Should().BeFalse();
        pub3Params[2].Name.Should().Be("typeName");
        pub3Params[2].ParameterType.Should().Be(typeof(string));
        pub3Params[2].IsOptional.Should().BeTrue();
        pub3Params[2].HasDefaultValue.Should().BeTrue();
        pub3Params[2].DefaultValue.Should().BeNull();

        // --- CreatePublisher (5-param overload) ---
        var pub5 = methods[1];
        pub5.Name.Should().Be("CreatePublisher");
        pub5.IsGenericMethod.Should().BeTrue();
        pub5.GetGenericArguments().Length.Should().Be(1);
        pub5.ReturnType.GetGenericTypeDefinition().Should().Be(typeof(Publisher<>));
        var pub5Params = pub5.GetParameters();
        pub5Params.Length.Should().Be(5);
        pub5Params[0].Name.Should().Be("topicName");
        pub5Params[0].ParameterType.Should().Be(typeof(string));
        pub5Params[1].Name.Should().Be("serializer");
        pub5Params[1].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(ICdrSerializer<>));
        pub5Params[2].Name.Should().Be("reliability");
        pub5Params[2].ParameterType.Should().Be(typeof(ReliabilityQos));
        pub5Params[3].Name.Should().Be("durability");
        pub5Params[3].ParameterType.Should().Be(typeof(DurabilityQos));
        pub5Params[4].Name.Should().Be("typeName");
        pub5Params[4].ParameterType.Should().Be(typeof(string));
        pub5Params[4].IsOptional.Should().BeTrue();
        pub5Params[4].HasDefaultValue.Should().BeTrue();
        pub5Params[4].DefaultValue.Should().BeNull();

        // --- CreateServiceClient ---
        var sc = methods[2];
        sc.Name.Should().Be("CreateServiceClient");
        sc.IsGenericMethod.Should().BeTrue();
        sc.GetGenericArguments().Length.Should().Be(2);
        sc.ReturnType.GetGenericTypeDefinition().Should().Be(typeof(ServiceClient<,>));
        var scParams = sc.GetParameters();
        scParams.Length.Should().Be(2);
        scParams[0].Name.Should().Be("descriptor");
        scParams[0].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(ServiceDescriptor<,>));
        scParams[0].IsOptional.Should().BeFalse();
        scParams[1].Name.Should().Be("serviceName");
        scParams[1].ParameterType.Should().Be(typeof(string));
        scParams[1].IsOptional.Should().BeFalse();

        // --- CreateSubscription (Action<T> overload, 5 params) ---
        var sub5 = methods[3];
        sub5.Name.Should().Be("CreateSubscription");
        sub5.IsGenericMethod.Should().BeTrue();
        sub5.GetGenericArguments().Length.Should().Be(1);
        sub5.ReturnType.GetGenericTypeDefinition().Should().Be(typeof(Subscription<>));
        var sub5Params = sub5.GetParameters();
        sub5Params.Length.Should().Be(5);
        sub5Params[0].Name.Should().Be("topicName");
        sub5Params[0].ParameterType.Should().Be(typeof(string));
        sub5Params[1].Name.Should().Be("serializer");
        sub5Params[1].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(ICdrSerializer<>));
        sub5Params[2].Name.Should().Be("handler");
        sub5Params[2].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(Action<>));
        sub5Params[3].Name.Should().Be("handlerContext");
        sub5Params[3].ParameterType.Should().Be(typeof(SynchronizationContext));
        sub5Params[3].IsOptional.Should().BeTrue();
        sub5Params[3].HasDefaultValue.Should().BeTrue();
        sub5Params[3].DefaultValue.Should().BeNull();
        sub5Params[4].Name.Should().Be("reliability");
        sub5Params[4].ParameterType.Should().Be(typeof(ReliabilityQos?));
        sub5Params[4].IsOptional.Should().BeTrue();
        sub5Params[4].HasDefaultValue.Should().BeTrue();
        sub5Params[4].DefaultValue.Should().BeNull();

        // --- CreateSubscription (Action<T,GuidPrefix> overload, 6 params) ---
        var sub6 = methods[4];
        sub6.Name.Should().Be("CreateSubscription");
        sub6.IsGenericMethod.Should().BeTrue();
        sub6.GetGenericArguments().Length.Should().Be(1);
        sub6.ReturnType.GetGenericTypeDefinition().Should().Be(typeof(Subscription<>));
        var sub6Params = sub6.GetParameters();
        sub6Params.Length.Should().Be(6);
        sub6Params[0].Name.Should().Be("topicName");
        sub6Params[0].ParameterType.Should().Be(typeof(string));
        sub6Params[1].Name.Should().Be("serializer");
        sub6Params[1].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(ICdrSerializer<>));
        sub6Params[2].Name.Should().Be("handler");
        sub6Params[2].ParameterType.GetGenericTypeDefinition().Should().Be(typeof(Action<,>));
        sub6Params[3].Name.Should().Be("typeName");
        sub6Params[3].ParameterType.Should().Be(typeof(string));
        sub6Params[3].IsOptional.Should().BeTrue();
        sub6Params[3].HasDefaultValue.Should().BeTrue();
        sub6Params[3].DefaultValue.Should().BeNull();
        sub6Params[4].Name.Should().Be("handlerContext");
        sub6Params[4].ParameterType.Should().Be(typeof(SynchronizationContext));
        sub6Params[4].IsOptional.Should().BeTrue();
        sub6Params[4].HasDefaultValue.Should().BeTrue();
        sub6Params[4].DefaultValue.Should().BeNull();
        sub6Params[5].Name.Should().Be("reliability");
        sub6Params[5].ParameterType.Should().Be(typeof(ReliabilityQos?));
        sub6Params[5].IsOptional.Should().BeTrue();
        sub6Params[5].HasDefaultValue.Should().BeTrue();
        sub6Params[5].DefaultValue.Should().BeNull();

        // --- CreateTopicDiagnostics ---
        var ctd = methods[5];
        ctd.Name.Should().Be("CreateTopicDiagnostics");
        ctd.IsGenericMethod.Should().BeFalse();
        ctd.ReturnType.Should().Be(typeof(TopicDiagnostics));
        ctd.GetParameters().Should().BeEmpty();

        // --- Dispose ---
        var disp = methods[6];
        disp.Name.Should().Be("Dispose");
        disp.IsGenericMethod.Should().BeFalse();
        disp.ReturnType.Should().Be(typeof(void));
        disp.GetParameters().Should().BeEmpty();
    }

    [Fact]
    public void Node_公開メソッドのnullableMetadata()
    {
        var t = typeof(Node);
        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName)
            .OrderBy(m => m.Name)
            .ThenBy(m => m.GetParameters().Length)
            .ToArray();

        // Parameter nullable metadata table:
        // (methodName, paramCount, paramIndex, paramName, isRefNullable, isOptional, hasDefault, defaultValue)
        var paramExpectations = new (string, int, int, string, bool, bool, bool, object?)[]
        {
            ("CreatePublisher", 3, 2, "typeName", true, true, true, null),
            ("CreatePublisher", 5, 4, "typeName", true, true, true, null),
            ("CreateSubscription", 6, 3, "typeName", true, true, true, null),
            ("CreateSubscription", 6, 4, "handlerContext", true, true, true, null),
            ("CreateSubscription", 5, 3, "handlerContext", true, true, true, null),
        };

        // Return type nullable table:
        // (methodName, paramCount, isVoid, isNullable)
        var returnExpectations = new (string, int, bool, bool)[]
        {
            ("CreatePublisher", 3, false, false),
            ("CreatePublisher", 5, false, false),
            ("CreateSubscription", 6, false, false),
            ("CreateSubscription", 5, false, false),
            ("CreateServiceClient", 2, false, false),
            ("CreateTopicDiagnostics", 0, false, false),
            ("Dispose", 0, true, false),
        };

        foreach (var (methodName, paramCount, paramIndex, paramName, isRefNullable, isOptional, hasDefault, defaultValue) in paramExpectations)
        {
            var method = methods.First(m => m.Name == methodName && m.GetParameters().Length == paramCount);
            var param = method.GetParameters()[paramIndex];
            param.Name.Should().Be(paramName,
                $"for {methodName}({paramCount} params) param[{paramIndex}]");
            param.IsOptional.Should().Be(isOptional,
                $"for {methodName}({paramCount} params) param[{paramIndex}] optional");
            param.HasDefaultValue.Should().Be(hasDefault,
                $"for {methodName}({paramCount} params) param[{paramIndex}] hasDefault");
            param.DefaultValue.Should().Be(defaultValue,
                $"for {methodName}({paramCount} params) param[{paramIndex}] default");

            if (isRefNullable)
            {
                var nullableFlags = GetNullableFlags(param);
                nullableFlags.Should().NotBeNull(
                    $"for {methodName}({paramCount} params) param[{paramIndex}] {paramName} must have NullableAttribute");
                nullableFlags.Should().Equal(new byte[] { 2 },
                    $"for {methodName}({paramCount} params) param[{paramIndex}] {paramName} must be Nullable(2)");
            }
        }

        foreach (var (methodName, paramCount, isVoid, isNullable) in returnExpectations)
        {
            var method = methods.First(m => m.Name == methodName && m.GetParameters().Length == paramCount);
            if (isVoid)
            {
                method.ReturnType.Should().Be(typeof(void));
                continue;
            }
            if (isNullable)
            {
                var nullableFlags = GetNullableFlags(method.ReturnParameter);
                nullableFlags.Should().NotBeNull(
                    $"for {methodName} return must have NullableAttribute");
                nullableFlags.Should().Equal(new byte[] { 2 },
                    $"for {methodName} return must be Nullable(2)");
            }
            else
            {
                var nullableFlags = GetNullableFlags(method.ReturnParameter);
                nullableFlags.Should().BeNull(
                    $"for {methodName} return must not have NullableAttribute");
            }
        }
    }

    private static byte[]? GetNullableFlags(ICustomAttributeProvider provider)
    {
        var attr = provider.GetCustomAttributes(false)
            .FirstOrDefault(a => a.GetType().Name == "NullableAttribute");
        if (attr is null) return null;
        var field = attr.GetType().GetField("NullableFlags", BindingFlags.Public | BindingFlags.Instance);
        return (byte[]?)field?.GetValue(attr);
    }

    [Fact]
    public void TopicDiagnostics_GetTopicInfo_nullableReturn()
    {
        var method = typeof(TopicDiagnostics).GetMethods(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .First(m => m.Name == "GetTopicInfo");
        method.ReturnType.Should().Be(typeof(TopicInfo));
        method.ReturnParameter.IsOptional.Should().BeFalse();
        var nullableAttr = method.ReturnParameter.GetCustomAttributes(false)
            .FirstOrDefault(a => a.GetType().Name == "NullableAttribute");
        nullableAttr.Should().NotBeNull("GetTopicInfo returns nullable TopicInfo?");
        // Verify NullableAttribute byte value = 2 (nullable)
        var flagsField = nullableAttr!.GetType().GetField("NullableFlags", BindingFlags.Public | BindingFlags.Instance);
        var flags = (byte[]?)flagsField?.GetValue(nullableAttr);
        flags.Should().Equal(new byte[] { 2 }, "GetTopicInfo return must have Nullable(2) = nullable");
    }

    [Fact]
    public void TopicDiagnostics_GetTopics_nonNullReturn()
    {
        var method = typeof(TopicDiagnostics).GetMethods(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .First(m => m.Name == "GetTopics");
        method.ReturnType.Should().Be(typeof(IReadOnlyList<TopicInfo>));
        var nullableAttr = method.ReturnParameter.GetCustomAttributes(false)
            .FirstOrDefault(a => a.GetType().Name == "NullableAttribute");
        nullableAttr.Should().BeNull("GetTopics returns non-nullable IReadOnlyList<TopicInfo>");
    }

    [Fact]
    public void TopicDiagnostics_CreateFrequencyMonitor_nullableOptionalDefault()
    {
        var method = typeof(TopicDiagnostics).GetMethods(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .First(m => m.Name == "CreateFrequencyMonitor");
        var optionsParam = method.GetParameters().First(p => p.Name == "options");
        optionsParam.IsOptional.Should().BeTrue();
        optionsParam.HasDefaultValue.Should().BeTrue();
        optionsParam.DefaultValue.Should().BeNull();
    }

    // ======== Spec Review: IClock precision (concrete value assertions) ========

    [Fact]
    public void GetElapsedTime_通常差で正しいTimeSpan()
    {
        var clock = new MockClock();
        long t0 = clock.GetTimestamp();
        long t1 = clock.Advance(1_000_000); // +1_000_000 ticks = 100ms
        clock.GetElapsedTime(t0, t1).Should().Be(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void GetElapsedTime_同timestampでTimeSpanZero()
    {
        var clock = SystemClock.Instance;
        long ts = clock.GetTimestamp();
        clock.GetElapsedTime(ts, ts).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void GetElapsedTime_longMax近傍でdoubleだと失敗する具体値()
    {
        var clock = new MockClock();
        clock.Advance(long.MaxValue - 200_000);
        long start = clock.GetTimestamp();
        long end = clock.Advance(100_000); // +100_000 ticks
        // old double: (double)end - (double)start loses precision → 0 → TimeSpan.Zero
        // decimal: exact 100_000 ticks = 10ms
        clock.GetElapsedTime(start, end).Should().Be(TimeSpan.FromTicks(100_000));
    }

    [Fact]
    public void GetElapsedTime_逆順でTimeSpanZero()
    {
        var clock = SystemClock.Instance;
        long t0 = clock.GetTimestamp();
        clock.GetElapsedTime(t0 + 1000, t0).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void GetElapsedTime_負のdeltaでZero()
    {
        var clock = SystemClock.Instance;
        clock.GetElapsedTime(100, 50).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void GetElapsedTime_longMin近傍からlongMax近傍でoverflowしない()
    {
        var clock = new MockClock();
        decimal delta = (decimal)long.MaxValue - (decimal)(long.MaxValue - 100_000);
        var elapsed = clock.GetElapsedTime(long.MaxValue - 100_000, long.MaxValue);
        // decimal handles this range without overflow; delta = 100_000 ticks
        elapsed.Should().Be(TimeSpan.FromTicks((long)delta));
    }

    [Fact]
    public void GetElapsedTime_longWrapで安全にZero()
    {
        var clock = SystemClock.Instance;
        clock.GetElapsedTime(long.MaxValue - 5, long.MinValue + 5).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void SystemClock_TicksToTimeSpan_knownValues()
    {
        // frequency = 10_000_000 Hz (100ns per tick, same as TimeSpan.TicksPerSecond)
        // Independent exact constants: 100_000 / 10_000_000 = 0.01s = 10ms
        SystemClock.TicksToTimeSpan(100_000, 10_000_000)
            .Should().Be(TimeSpan.FromMilliseconds(10));
        // 1_000_000 / 10_000_000 = 0.1s = 100ms
        SystemClock.TicksToTimeSpan(1_000_000, 10_000_000)
            .Should().Be(TimeSpan.FromMilliseconds(100));
        // 10_000_000 ticks at 50 MHz = 0.2s = 200ms
        SystemClock.TicksToTimeSpan(10_000_000, 50_000_000)
            .Should().Be(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void SystemClock_TicksToTimeSpan_longMax近傍でdouble退行しない()
    {
        // 100_000 ticks at 10 MHz = exactly 10ms
        // (double)long.MaxValue - (double)(long.MaxValue - 100_000) = 0.0 (precision loss)
        // decimal preserves exact 100_000 → 10ms
        long start = long.MaxValue - 100_000;
        long end = long.MaxValue;
        long delta = end - start;

        var elapsed = SystemClock.TicksToTimeSpan(delta, 10_000_000);

        // Independent expected constant: 100_000 / 10_000_000 = 10ms
        elapsed.Should().NotBe(TimeSpan.Zero, "double precision loss would return Zero");
        elapsed.Should().Be(TimeSpan.FromMilliseconds(10));
    }

    [Fact]
    public void SystemClock_TicksToTimeSpan_負のticksでZero()
    {
        SystemClock.TicksToTimeSpan(-1, 10_000_000).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void SystemClock_TicksToTimeSpan_zeroTicksでZero()
    {
        SystemClock.TicksToTimeSpan(0, 10_000_000).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void SystemClock_TicksToTimeSpan_zeroFrequencyでZero()
    {
        SystemClock.TicksToTimeSpan(100_000, 0).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void SystemClock_GetElapsedTime_longMinEndでTimeSpanZero()
    {
        var clock = SystemClock.Instance;
        clock.GetElapsedTime(0, long.MinValue).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void TicksToTimeSpan_overflowでArgumentOutOfRange()
    {
        // long.MaxValue ticks at 1 Hz → totalTicks = long.MaxValue * TicksPerSecond >> long.MaxValue → overflow
        var act = () => SystemClock.TicksToTimeSpan(long.MaxValue, 1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TicksToTimeSpan_TimeSpanMax境界()
    {
        // frequency = TicksPerSecond → totalTicks = long.MaxValue (independently verified as TimeSpan.MaxValue.Ticks)
        var result = SystemClock.TicksToTimeSpan(long.MaxValue, TimeSpan.TicksPerSecond);
        result.Ticks.Should().Be(TimeSpan.MaxValue.Ticks);
        result.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void TicksToTimeSpan_TimeSpanMax超過でArgumentOutOfRange()
    {
        // one tick more than max: totalTicks > long.MaxValue → throws
        // At frequency=1: ticks = long.MaxValue already overflows, test that directly
        // At frequency=TicksPerSecond/2: ticks = long.MaxValue → totalTicks = long.MaxValue * 2 > long.MaxValue
        var act = () => SystemClock.TicksToTimeSpan(long.MaxValue, TimeSpan.TicksPerSecond / 2);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void GetElapsedTime_delta超過longMaxでZero()
    {
        var clock = SystemClock.Instance;
        // delta = (decimal)long.MaxValue - (decimal)long.MinValue > long.MaxValue → returns Zero
        clock.GetElapsedTime(long.MinValue, long.MaxValue).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void GetStatistics_longMax近傍でrateHzが正しい()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 100);

        clock.Advance(long.MaxValue - 10_000_000);
        for (int i = 0; i < 5; i++)
            monitor.Record(clock.Advance(1_000_000)); // 100ms intervals

        var stats = monitor.GetStatistics();
        stats.HasData.Should().BeTrue();
        // 4 intervals × 100ms = 400ms → 10Hz
        stats.RateHz.Should().BeApproximately(10.0, 0.001);
        stats.WindowDuration.Should().Be(TimeSpan.FromMilliseconds(400));
    }

    [Fact]
    public void GetStatistics_interval合計がoverflowしない()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 1000);
        long step = 1_000_000_000_000; // 10^12 ticks per interval
        for (int i = 0; i < 1000; i++)
            monitor.Record(clock.Advance(step));

        var stats = monitor.GetStatistics();
        stats.HasData.Should().BeTrue();
        stats.SampleCount.Should().Be(1000);
        stats.MeanInterval.Ticks.Should().Be(step);
    }

    // ======== Spec Review: WaitForMatchedAsync deadline (fake monotonic clock) ========

    [Fact]
    public async Task WaitForMatchedAsync_readerのみ存在でtimeoutがfalse()
    {
        using var context = CreateContext();
        context.Start();
        using var node = new Node(context, "reader_only_node");
        using var diag = node.CreateTopicDiagnostics();

        var prefix = Prefix(55);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Reader, 0x10, "rt/reader_only_topic"), DateTime.UtcNow);

        using var monitor = diag.CreateFrequencyMonitor("/reader_only_topic");

        (await monitor.WaitForMatchedAsync(1, TimeSpan.FromMilliseconds(1))).Should().BeFalse();
    }

    [Fact]
    public async Task WaitForMatchedAsync_外部キャンセルでOperationCanceledException()
    {
        using var context = CreateContext();
        using var node = new Node(context, "ext_cancel_node");
        using var diag = node.CreateTopicDiagnostics();

        var prefix = Prefix(51);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/ext_cancel_topic"), DateTime.UtcNow);
        using var monitor = diag.CreateFrequencyMonitor("/ext_cancel_topic");

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => monitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(5), cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task WaitForMatchedAsync_fakeDeadline前後で動作する()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var waitTask = monitor.WaitForMatchedAsync(1, TimeSpan.FromMilliseconds(100));

        // deadline前: clock within deadline (99ms < 100ms)
        clock.Advance(990_000);
        await Task.Delay(10);
        waitTask.IsCompleted.Should().BeFalse("deadline内なので待機中");

        // deadline後: clock past deadline (+110ms > 100ms)
        clock.Advance(110_000);
        // bounded wait: old CancelAfter implementation would hang here
        var completed = await waitTask.WaitAsync(TimeSpan.FromMilliseconds(100));
        completed.Should().BeFalse("deadline超過でタイムアウト");
    }

    [Fact]
    public async Task WaitForMatchedAsync_TimeSpanMaxValueはInfinite相当()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var task = monitor.WaitForMatchedAsync(1, TimeSpan.MaxValue);

        // Advance clock far past any reasonable deadline → should NOT time out
        clock.Advance(long.MaxValue / 2);
        await Task.Delay(10);

        task.IsCompleted.Should().BeFalse("TimeSpan.MaxValueは無限期待機");

        monitor.Dispose();
        var ex = await Record.ExceptionAsync(() => task).WaitAsync(TimeSpan.FromSeconds(5));
        ex.Should().BeOfType<ObjectDisposedException>();
    }

    [Fact]
    public async Task WaitForMatchedAsync_InfiniteTimeSpanは永久待機()
    {
        var clock = new MockClock();
        using var monitor = CreateMonitorWithClock(clock, windowSize: 10);

        var task = monitor.WaitForMatchedAsync(1, System.Threading.Timeout.InfiniteTimeSpan);

        clock.Advance(long.MaxValue / 2);
        await Task.Delay(10);

        task.IsCompleted.Should().BeFalse("InfiniteTimeSpanは無限期待機");

        monitor.Dispose();
        var ex = await Record.ExceptionAsync(() => task).WaitAsync(TimeSpan.FromSeconds(5));
        ex.Should().BeOfType<ObjectDisposedException>();
    }

    // ======== Spec Review: Node.Dispose 順序 ========

    [Fact]
    public void Node_Disposeは診断_wrapper_endpointの順で処理する()
    {
        using var context = CreateContext();
        context.Start();
        var node = new Node(context, "dispose_order_test");

        var recordedEvents = new List<string>();
        node.TestEventRecorder = msg => recordedEvents.Add(msg);

        using var pub = node.CreatePublisher<StringMessage>(
            "order_topic", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var diag = node.CreateTopicDiagnostics();
        using var monitor = diag.CreateFrequencyMonitor("/order_topic");

        node.Dispose();

        var idxDiagStart = recordedEvents.IndexOf("NodeDisposeDiagnosticsStart");
        var idxDiagEnd = recordedEvents.IndexOf("TopicDiagnosticsDisposeStart");
        var idxTopicDiagEnd = recordedEvents.IndexOf("TopicDiagnosticsDisposeEnd");
        var idxDiagSectionEnd = recordedEvents.IndexOf("NodeDisposeDiagnosticsEnd");
        var idxWrappersStart = recordedEvents.IndexOf("NodeDisposeWrappersStart");
        var idxWrappersEnd = recordedEvents.IndexOf("NodeDisposeWrappersEnd");
        var idxEndpointsStart = recordedEvents.IndexOf("NodeDisposeEndpointsStart");
        var idxEndpointsEnd = recordedEvents.IndexOf("NodeDisposeEndpointsEnd");

        idxDiagStart.Should().BeGreaterOrEqualTo(0, "must have diagnostics start");
        idxDiagEnd.Should().BeGreaterOrEqualTo(0, "must have TopicDiagnosticsDisposeStart");
        idxTopicDiagEnd.Should().BeGreaterOrEqualTo(0, "must have TopicDiagnosticsDisposeEnd");
        idxDiagSectionEnd.Should().BeGreaterOrEqualTo(0, "must have diagnostics end");
        idxWrappersStart.Should().BeGreaterOrEqualTo(0, "must have wrappers start");
        idxWrappersEnd.Should().BeGreaterOrEqualTo(0, "must have wrappers end");
        idxEndpointsStart.Should().BeGreaterOrEqualTo(0, "must have endpoints start");
        idxEndpointsEnd.Should().BeGreaterOrEqualTo(0, "must have endpoints end");

        // Verify order: diagnostics → wrappers → endpoints
        idxDiagSectionEnd.Should().BeLessThan(idxWrappersStart,
            "diagnostics dispose must complete before wrappers dispose");
        idxWrappersEnd.Should().BeLessThan(idxEndpointsStart,
            "wrappers dispose must complete before endpoints unregister");
    }

    [Fact]
    public async Task Node_Disposeでdiag先にDisposeされmonitorのwaiterがキャンセルされる()
    {
        using var context = CreateContext();
        context.Start();
        var node = new Node(context, "diag_first_test");

        var recordedEvents = new List<string>();
        node.TestEventRecorder = msg => recordedEvents.Add(msg);

        using var diag = node.CreateTopicDiagnostics();

        var prefix = Prefix(60);
        context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
        context.DiscoveryDb.UpsertEndpoint(
            Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/diag_first"), DateTime.UtcNow);
        using var monitor = diag.CreateFrequencyMonitor("/diag_first");

        // Start waiting (will not be satisfied)
        var waitTask = monitor.WaitForMatchedAsync(2, TimeSpan.FromSeconds(10));

        node.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => waitTask);

        // Verify event ordering: diagnostics → wrappers → endpoints
        var idxDiagStart = recordedEvents.IndexOf("NodeDisposeDiagnosticsStart");
        var idxDiagEnd = recordedEvents.IndexOf("TopicDiagnosticsDisposeStart");
        var idxTopicDiagEnd = recordedEvents.IndexOf("TopicDiagnosticsDisposeEnd");
        var idxDiagSectionEnd = recordedEvents.IndexOf("NodeDisposeDiagnosticsEnd");
        var idxWrappersStart = recordedEvents.IndexOf("NodeDisposeWrappersStart");
        var idxEndpointsStart = recordedEvents.IndexOf("NodeDisposeEndpointsStart");

        idxDiagStart.Should().BeGreaterOrEqualTo(0);
        idxDiagEnd.Should().BeGreaterOrEqualTo(0);
        idxTopicDiagEnd.Should().BeGreaterOrEqualTo(0);
        idxDiagSectionEnd.Should().BeGreaterOrEqualTo(0);
        idxWrappersStart.Should().BeGreaterOrEqualTo(0);
        idxEndpointsStart.Should().BeGreaterOrEqualTo(0);

        // TopicDiagnosticsDisposeStart must be before TopicDiagnosticsDisposeEnd
        idxDiagEnd.Should().BeLessThan(idxTopicDiagEnd);
        // TopicDiagnosticsDisposeStart/End must be inside diagnostics section
        idxDiagEnd.Should().BeGreaterThan(idxDiagStart);
        idxTopicDiagEnd.Should().BeLessThan(idxDiagSectionEnd);
        // Diagnostics section must complete before wrappers
        idxDiagSectionEnd.Should().BeLessThan(idxWrappersStart);
        // Wrappers must complete before endpoints
        idxWrappersStart.Should().BeLessThan(idxEndpointsStart);
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
        var opts = new TopicFrequencyOptions { WindowSize = windowSize };
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
            if (endingTimestamp < startingTimestamp)
                return TimeSpan.Zero;
            decimal delta = (decimal)endingTimestamp - (decimal)startingTimestamp;
            if (delta <= 0)
                return TimeSpan.Zero;
            return TimeSpan.FromTicks((long)delta);
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
