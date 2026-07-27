using System.Collections.Concurrent;
using System.Net;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Dds;
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Discovery;
using ROSettaDDS.Msgs.Std;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Naming;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rtps.HistoryCache;
using ROSettaDDS.Rtps.Writer;
using ROSettaDDS.Transport;
using Guid = ROSettaDDS.Common.Guid;
using Xunit;

namespace ROSettaDDS.Tests.Rcl;

public class NodeTests
{
    [Fact]
    public void コンストラクタは_Context_参照と_Name_を保持する()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        using var node = new Node(ctx, "chatter_talker");

        Assert.Same(ctx, node.Context);
        Assert.Equal("chatter_talker", node.Name);
    }

    [Fact]
    public void null_context_を渡すと_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new Node(null!, "name"));
    }

    [Fact]
    public void null_name_を渡すと_ArgumentException()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        Assert.Throws<ArgumentException>(() => new Node(ctx, null!));
    }

    [Fact]
    public void CreatePublisher_が_Publisher_を返し_Dispose_後に例外を投げる()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "talker");
        try
        {
            using var pub = node.CreatePublisher<StringMessage>(
                "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
            Assert.NotNull(pub);
        }
        finally { node.Dispose(); }

        Assert.Throws<ObjectDisposedException>(() =>
            node.CreatePublisher<StringMessage>("chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName));
    }

    [Fact]
    public void CreateSubscription_が_Subscription_を返す()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "listener");
        using var sub = node.CreateSubscription<StringMessage>(
            "chatter",
            StringMessageSerializer.Instance,
            (msg) => { });
        Assert.NotNull(sub);
    }

    [Fact]
    public async Task 異なる_Context_上の_2_Node_で_Pub_Sub_できる()
    {
        var opts = new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
            SpdpInterval = TimeSpan.FromMilliseconds(50),
            SedpInterval = TimeSpan.FromMilliseconds(50),
        };
        using var talkerCtx = new Context(opts);
        using var listenerCtx = new Context(opts);
        talkerCtx.Start();
        listenerCtx.Start();

        using var talker = new Node(talkerCtx, "talker");
        using var listener = new Node(listenerCtx, "listener");

        var topicName = $"chatter_{System.Guid.NewGuid():N}";
        var received = new System.Collections.Concurrent.ConcurrentQueue<string>();
        using var sub = listener.CreateSubscription<StringMessage>(
            topicName, StringMessageSerializer.Instance,
            (msg, _) => received.Enqueue(msg.Data),
            typeName: StringMessage.DdsTypeName);
        using var pub = talker.CreatePublisher<StringMessage>(
            topicName, StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        // 両側でマッチしてから配信
        await sub.WaitForMatchedAsync(1, TimeSpan.FromSeconds(10));
        await pub.WaitForMatchedAsync(1, TimeSpan.FromSeconds(10));

        await pub.PublishAsync(new StringMessage("hello"));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
        while (received.IsEmpty && DateTime.UtcNow < deadline)
            Thread.Sleep(50);

        Assert.False(received.IsEmpty);
        Assert.Equal("hello", received.First());
    }

    [Fact]
    public void CreatePublisher_Phase1後_Disposeが割り込むとpending完了待ちしてcleanupする()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "talker");

        var phase1Done = new ManualResetEventSlim();
        var resumeCreate = new ManualResetEventSlim();
        Exception? createError = null;
        Exception? disposeError = null;
        int disposeCompleted = 0;

        node.BeforeDisposedCheckCallback = () =>
        {
            phase1Done.Set();
            resumeCreate.Wait();
        };

        var waitLoopEntered = new ManualResetEventSlim();
        int? observedPendingCount = null;
        node.PendingRegistrationsWaitLoopEntered = (pendingCount) =>
        {
            observedPendingCount = pendingCount;
            waitLoopEntered.Set();
        };

        var createThread = new Thread(() =>
        {
            try
            {
                node.CreatePublisher<StringMessage>(
                    "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
            }
            catch (Exception ex)
            {
                createError = ex;
            }
        });
        createThread.Start();

        Assert.True(phase1Done.Wait(TimeSpan.FromSeconds(5)),
            "phase 1 (metadata registration) must complete");

        // 別スレッドから本物の Dispose() を呼ぶ
        var disposeThread = new Thread(() =>
        {
            try
            {
                node.Dispose();
                Interlocked.Exchange(ref disposeCompleted, 1);
            }
            catch (Exception ex)
            {
                disposeError = ex;
            }
        });
        disposeThread.Start();

        Assert.True(waitLoopEntered.Wait(TimeSpan.FromSeconds(5)),
            "Dispose must enter pending registration wait loop");
        Assert.True(node.IsDisposed);
        Assert.True(observedPendingCount.HasValue);
        Assert.True(observedPendingCount!.Value > 0,
            "callback must receive a pending count greater than 0");

        // pending registration が残っているため Dispose はまだ完了しない
        Assert.Equal(0, Volatile.Read(ref disposeCompleted));

        // Create スレッドを再開 → rollback → pending 解放
        resumeCreate.Set();
        Assert.True(createThread.Join(TimeSpan.FromSeconds(5)),
            "CreatePublisher thread must complete after rollback");

        // Dispose スレッドが SpinWait を抜けて cleanup 完了する
        Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)),
            "Dispose thread must complete after pending registration finishes");

        Assert.Null(disposeError);
        Assert.NotNull(createError);
        var odEx = Assert.IsType<ObjectDisposedException>(createError);
        Assert.Contains(typeof(Node).Name, odEx.ObjectName, StringComparison.Ordinal);

        // Node は完全に dispose され、Context からも解除されている
        Assert.True(node.IsDisposed);
        Assert.Throws<ObjectDisposedException>(() =>
            node.CreatePublisher<StringMessage>(
                "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName));

        node.BeforeDisposedCheckCallback = null;
        node.PendingRegistrationsWaitLoopEntered = null;
    }

    [Fact]
    public void BeginRegistrationがThrowIfDisposedとpending_incrementを原子的に行う()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "atomic_test");

        var createStarted = new ManualResetEventSlim();
        var resumeCreate = new ManualResetEventSlim();
        Exception? createError = null;

        int? pendingAtCallback = null;
        node.BeforeCreateStartCallback = () =>
        {
            pendingAtCallback = GetPendingRegistrationsField(node);
            createStarted.Set();
            resumeCreate.Wait();
        };

        var waitLoopEntered = new ManualResetEventSlim();
        int? observedPendingCount = null;
        node.PendingRegistrationsWaitLoopEntered = (pendingCount) =>
        {
            observedPendingCount = pendingCount;
            waitLoopEntered.Set();
        };

        var createThread = new Thread(() =>
        {
            try
            {
                node.CreatePublisher<StringMessage>(
                    "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
            }
            catch (Exception ex)
            {
                createError = ex;
            }
        });
        createThread.Start();

        Assert.True(createStarted.Wait(TimeSpan.FromSeconds(5)),
            "Create must reach callback after BeginRegistration");

        // BeginRegistration が完了 → disposed チェックとインクリメントが済んでいる
        Assert.True(pendingAtCallback.HasValue);
        Assert.True(pendingAtCallback!.Value > 0,
            $"pending must be > 0 after BeginRegistration (was {pendingAtCallback})");

        // Dispose を別スレッドから呼ぶ
        Exception? disposeError = null;
        var disposeThread = new Thread(() =>
        {
            try
            {
                node.Dispose();
            }
            catch (Exception ex)
            {
                disposeError = ex;
            }
        });
        disposeThread.Start();

        // Dispose は pending > 0 のため spin wait に入る
        Assert.True(waitLoopEntered.Wait(TimeSpan.FromSeconds(5)),
            "Dispose must enter pending registration wait loop");
        Assert.True(observedPendingCount.HasValue);
        Assert.True(observedPendingCount!.Value > 0,
            $"observed pending must be > 0 (was {observedPendingCount})");

        // Create スレッドを再開 → disposed チェックで ObjectDisposedException → rollback → pending=0
        resumeCreate.Set();
        Assert.True(createThread.Join(TimeSpan.FromSeconds(5)),
            "CreatePublisher thread must complete after rollback");

        Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)),
            "Dispose thread must complete after pending registration finishes");

        Assert.Null(disposeError);
        Assert.NotNull(createError);
        var odEx = Assert.IsType<ObjectDisposedException>(createError);
        Assert.Contains(typeof(Node).Name, odEx.ObjectName, StringComparison.Ordinal);

        Assert.True(node.IsDisposed);

        node.BeforeCreateStartCallback = null;
        node.PendingRegistrationsWaitLoopEntered = null;
    }

    [Fact]
    public void BeforeCreateStartCallbackが例外を投げてもDisposeが完了する()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "callback_ex_test");

        node.BeforeCreateStartCallback = () => throw new InvalidOperationException("callback failed");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            node.CreatePublisher<StringMessage>(
                "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName));
        Assert.Equal("callback failed", ex.Message);

        Assert.Equal(0, GetPendingRegistrationsField(node));

        node.Dispose();
        Assert.True(node.IsDisposed);

        node.BeforeCreateStartCallback = null;
    }

    [Fact]
    public void CreatePublisher_phase2_match_failure_rolls_back()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "pub_test");

        using var sub = node.CreateSubscription<StringMessage>(
            "chatter", StringMessageSerializer.Instance, _ => { });

        var reader = node.Snapshot().Readers[0];
        reader.Dispose();

        var ex = Assert.Throws<ObjectDisposedException>(() =>
            node.CreatePublisher<StringMessage>("chatter", StringMessageSerializer.Instance));

        var snapshot = node.Snapshot();
        snapshot.Writers.Should().BeEmpty();
        snapshot.Readers.Should().Contain(reader);
        reader.MatchedWriterCount.Should().Be(0, "failed writer must be unmatched from existing reader");

        using var pub2 = node.CreatePublisher<StringMessage>(
            "other", StringMessageSerializer.Instance);
        Assert.NotNull(pub2);
    }

    [Fact]
    public void CreateSubscription_phase2_match_failure_rolls_back()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "sub_test");

        using var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var writer = node.Snapshot().Writers[0];
        writer.Dispose();

        var ex = Assert.Throws<ObjectDisposedException>(() =>
            node.CreateSubscription<StringMessage>("chatter", StringMessageSerializer.Instance, _ => { }));

        var snapshot = node.Snapshot();
        snapshot.Readers.Should().BeEmpty();
        snapshot.Writers.Should().Contain(writer);
        writer.MatchedReaderCount.Should().Be(0, "failed reader must be unmatched from existing writer");

        using var sub2 = node.CreateSubscription<StringMessage>(
            "other", StringMessageSerializer.Instance, _ => { });
        Assert.NotNull(sub2);
    }

    [Fact]
    public void CreateServiceClient_reply_reader_phase2_match_failure_rolls_back()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_test");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var replyTopic = TopicNameMangler.MangleServiceReply(svcName);

        var writerGuid = new Guid(ctx.GuidPrefix, ctx.UserEntityIds.AllocateWriter());
        var transport = new SilentTransport();
        var history = new WriterHistoryCache(writerGuid);
        var replyWriter = new StatefulWriter(
            transport,
            Locator.FromUdpV4(IPAddress.Loopback, 7401),
            ProtocolVersion.Current,
            VendorId.ROSettaDDS,
            ctx.GuidPrefix,
            writerGuid.EntityId,
            TimeSpan.FromSeconds(1),
            history,
            NullLogger.Instance);

        var endpointData = new DiscoveredEndpointData
        {
            Kind = EndpointKind.Writer,
            EndpointGuid = writerGuid,
            ParticipantGuid = ctx.Guid,
            TopicName = replyTopic,
            TypeName = "std_msgs::msg::dds_::String_",
            Reliability = ReliabilityQos.Reliable,
            Durability = DurabilityQos.Volatile,
        };
        endpointData.UnicastLocators.Add(Locator.FromUdpV4(IPAddress.Loopback, 7411));

        node.RegisterWriterMetadataForTest(endpointData, replyWriter);
        replyWriter.Dispose();

        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: "std_msgs::msg::dds_::String_",
            responseDdsTypeName: "std_msgs::msg::dds_::String_",
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var ex = Assert.Throws<ObjectDisposedException>(() =>
            node.CreateServiceClient(descriptor, svcName));

        var snapshot = node.Snapshot();
        snapshot.Readers.Should().BeEmpty();
        snapshot.Writers.Should().HaveCount(1,
            "manually injected reply writer must remain; request publisher must be rolled back");

        using var sub2 = node.CreateSubscription<StringMessage>(
            "other", StringMessageSerializer.Instance, _ => { });
        Assert.NotNull(sub2);
    }

    [Fact]
    public void CreateServiceClient_中で_Dispose_が割り込むとpending完了待ちしてrollbackする()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "svc_concurrent_test");

        var phase1Done = new ManualResetEventSlim();
        var resumeCreate = new ManualResetEventSlim();
        Exception? createError = null;
        Exception? disposeError = null;
        int disposeCompleted = 0;

        node.BeforeDisposedCheckCallback = () =>
        {
            phase1Done.Set();
            resumeCreate.Wait();
        };

        var waitLoopEntered = new ManualResetEventSlim();
        int? observedPendingCount = null;
        node.PendingRegistrationsWaitLoopEntered = (pendingCount) =>
        {
            observedPendingCount = pendingCount;
            waitLoopEntered.Set();
        };

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var createThread = new Thread(() =>
        {
            try
            {
                using var client = node.CreateServiceClient(descriptor, svcName);
            }
            catch (Exception ex)
            {
                createError = ex;
            }
        });
        createThread.Start();

        Assert.True(phase1Done.Wait(TimeSpan.FromSeconds(5)),
            "phase 1 (metadata registration) must complete");

        var disposeThread = new Thread(() =>
        {
            try
            {
                node.Dispose();
                Interlocked.Exchange(ref disposeCompleted, 1);
            }
            catch (Exception ex)
            {
                disposeError = ex;
            }
        });
        disposeThread.Start();

        Assert.True(waitLoopEntered.Wait(TimeSpan.FromSeconds(5)),
            "Dispose must enter pending registration wait loop");
        Assert.True(node.IsDisposed);
        Assert.True(observedPendingCount.HasValue);
        Assert.True(observedPendingCount!.Value > 0,
            "callback must receive a pending count greater than 0");

        Assert.Equal(0, Volatile.Read(ref disposeCompleted));

        resumeCreate.Set();
        Assert.True(createThread.Join(TimeSpan.FromSeconds(5)),
            "CreateServiceClient thread must complete after rollback");

        Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)),
            "Dispose thread must complete after pending registration finishes");

        Assert.Null(disposeError);
        Assert.NotNull(createError);
        var odEx = Assert.IsType<ObjectDisposedException>(createError);
        Assert.Contains(typeof(Node).Name, odEx.ObjectName, StringComparison.Ordinal);

        Assert.True(node.IsDisposed);

        node.BeforeDisposedCheckCallback = null;
        node.PendingRegistrationsWaitLoopEntered = null;
    }

    [Fact]
    public void CreateServiceClient_outer_only_pending_で_Dispose_が割り込むと待機してrollbackする()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "svc_outer_test");

        var pausePoint = new ManualResetEventSlim();
        var resumeCreate = new ManualResetEventSlim();
        Exception? createError = null;
        Exception? disposeError = null;
        int disposeCompleted = 0;

        node.BeforeServiceReplyReaderCreateCallback = () =>
        {
            pausePoint.Set();
            resumeCreate.Wait();
        };

        var waitLoopEntered = new ManualResetEventSlim();
        int? observedPendingCount = null;
        node.PendingRegistrationsWaitLoopEntered = (pendingCount) =>
        {
            observedPendingCount = pendingCount;
            waitLoopEntered.Set();
        };

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var createThread = new Thread(() =>
        {
            try
            {
                using var client = node.CreateServiceClient(descriptor, svcName);
            }
            catch (Exception ex)
            {
                createError = ex;
            }
        });
        createThread.Start();

        Assert.True(pausePoint.Wait(TimeSpan.FromSeconds(5)),
            "BeforeServiceReplyReaderCreateCallback must fire");

        var disposeThread = new Thread(() =>
        {
            try
            {
                node.Dispose();
                Interlocked.Exchange(ref disposeCompleted, 1);
            }
            catch (Exception ex)
            {
                disposeError = ex;
            }
        });
        disposeThread.Start();

        Assert.True(waitLoopEntered.Wait(TimeSpan.FromSeconds(5)),
            "Dispose must enter pending registration wait loop");
        Assert.True(node.IsDisposed);
        Assert.True(observedPendingCount.HasValue);
        Assert.Equal(1, observedPendingCount!.Value);

        Assert.Equal(0, Volatile.Read(ref disposeCompleted));

        resumeCreate.Set();
        Assert.True(createThread.Join(TimeSpan.FromSeconds(5)),
            "CreateServiceClient thread must complete after rollback");

        Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)),
            "Dispose thread must complete after pending registration finishes");

        Assert.Null(disposeError);
        Assert.NotNull(createError);
        var odEx = Assert.IsType<ObjectDisposedException>(createError);
        Assert.Contains(typeof(Node).Name, odEx.ObjectName, StringComparison.Ordinal);

        Assert.True(node.IsDisposed);

        node.BeforeServiceReplyReaderCreateCallback = null;
        node.PendingRegistrationsWaitLoopEntered = null;
    }

    [Fact]
    public void 明示DisposeされたPublisherが_trackedWrappers_から削除される()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "pub_tracking_test");

        for (int i = 0; i < 5; i++)
        {
            var pub = node.CreatePublisher<StringMessage>(
                $"chatter_{i}", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
            Assert.Equal(1, node.TrackedWrapperCount);
            pub.Dispose();
            Assert.Equal(0, node.TrackedWrapperCount);
        }

        using var pub2 = node.CreatePublisher<StringMessage>(
            "final", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        Assert.Equal(1, node.TrackedWrapperCount);

        node.Dispose();
        Assert.Equal(0, node.TrackedWrapperCount);
    }

    [Fact]
    public void Publisherがtracked時にpendingが0より大きい()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "pub_pending_boundary_test");

        var trackedFired = new ManualResetEventSlim();
        int pendingAtTrackTime = 0;
        int trackedCountAtTrackTime = 0;

        node.AfterWrapperTracked = () =>
        {
            pendingAtTrackTime = GetPendingRegistrationsField(node);
            trackedCountAtTrackTime = node.TrackedWrapperCount;
            trackedFired.Set();
        };

        using var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        Assert.True(trackedFired.Wait(TimeSpan.FromSeconds(5)),
            "AfterWrapperTracked must fire when publisher is tracked");
        Assert.True(pendingAtTrackTime > 0,
            $"pending must be > 0 when tracked (was {pendingAtTrackTime})");
        Assert.Equal(1, trackedCountAtTrackTime);

        Assert.Equal(1, node.TrackedWrapperCount);

        node.Dispose();
        Assert.Equal(0, node.TrackedWrapperCount);
    }

    [Fact]
    public async Task Publisher_DisposeとNode_Disposeの同時実行が安全()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "race_test");

        var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var race1 = Task.Run(() => pub.Dispose());
        var race2 = Task.Run(() => node.Dispose());

        await Task.WhenAll(race1, race2);
        Assert.True(node.IsDisposed);
    }

    [Fact]
    public void SubscriptionのhandlerContextがSynchronizationContextを保持する()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "syncctx_test");

        var syncCtx = new CapturingSynchronizationContext();
        using var sub = node.CreateSubscription<StringMessage>(
            "chatter", StringMessageSerializer.Instance, _ => { }, handlerContext: syncCtx);

        var storedCtx = typeof(Subscription<StringMessage>)
            .GetField("_handlerContext",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.GetValue(sub);
        storedCtx.Should().Be(syncCtx);
    }

    [Fact]
    public async Task ServiceClient_CallAsync_timeout後にpendingが残らない()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_pending_test");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        using var client = node.CreateServiceClient(descriptor, svcName);

        Assert.Equal(0, client.PendingRequestCount);

        var timeout = TimeSpan.FromMilliseconds(100);
        await Assert.ThrowsAsync<TimeoutException>(() =>
            client.CallAsync(new StringMessage("timeout"), timeout));
        Assert.Equal(0, client.PendingRequestCount);
    }

    [Fact]
    public void ServiceClient_Dispose時にpendingがcleanupされる()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_dispose_pending");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var client = node.CreateServiceClient(descriptor, svcName);

        var tcsField = typeof(ServiceClient<StringMessage, StringMessage>)
            .GetField("_pending",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var pending = (ConcurrentDictionary<SampleIdentity, TaskCompletionSource<StringMessage>>)
            tcsField.GetValue(client)!;
        var fakeKey = new SampleIdentity(new ROSettaDDS.Common.Guid(ctx.GuidPrefix, new EntityId(1, EntityKind.Unknown)), new SequenceNumber(99));
        pending[fakeKey] = new TaskCompletionSource<StringMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.Equal(1, client.PendingRequestCount);

        client.Dispose();
        Assert.Equal(0, client.PendingRequestCount);
    }

    [Fact]
    public async Task Publisher_DisposeとNode_Disposeの競合時advertise順序がALIVEからUNREGISTERED()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "race_test");

        var recordedEvents = new ConcurrentQueue<string>();
        node.TestEventRecorder = ev => recordedEvents.Enqueue(ev);

        using var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var race1 = Task.Run(() => pub.Dispose());
        var race2 = Task.Run(() => node.Dispose());

        await Task.WhenAll(race1, race2);
        Assert.True(node.IsDisposed);

        var eventsArray = recordedEvents.ToArray();
        var beforeUnregIdx = Array.IndexOf(eventsArray, "BeforeReceiverUnregisterWriter");
        var beforeSedpIdx = Array.IndexOf(eventsArray, "BeforeSedpUnregisterWriter");
        var afterSedpIdx = Array.IndexOf(eventsArray, "AfterSedpUnregisterWriter");

        Assert.True(beforeUnregIdx >= 0,
            $"BeforeReceiverUnregisterWriter not found in [{string.Join(", ", eventsArray)}]");
        Assert.True(beforeSedpIdx >= 0,
            $"BeforeSedpUnregisterWriter not found in [{string.Join(", ", eventsArray)}]");
        Assert.True(afterSedpIdx >= 0,
            $"AfterSedpUnregisterWriter not found in [{string.Join(", ", eventsArray)}]");
        Assert.True(beforeUnregIdx < beforeSedpIdx,
            $"receiver unregister must precede SEDP unregister; order: {string.Join(" -> ", eventsArray)}");
    }

    [Fact]
    public async Task ServiceClient_CallAsyncとDisposeの競合でpending管理が安全()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_race_test");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var client = node.CreateServiceClient(descriptor, svcName);

        var callTasks = Enumerable.Range(0, 20).Select(_ =>
            client.CallAsync(new StringMessage("test"), TimeSpan.FromMilliseconds(10))
                .ContinueWith(t => { }));

        var disposeTask = Task.Run(() => client.Dispose());

        await Task.WhenAll(
            Task.WhenAll(callTasks).ContinueWith(_ => { }),
            disposeTask);

        Assert.Equal(0, client.PendingRequestCount);
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.CallAsync(new StringMessage("after"), TimeSpan.FromMilliseconds(1)));
    }

    [Fact]
    public void SynchronousDisposeがNonPumpingSyncContext上でも完了する()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.SedpAdvertiseDelay = () => new ValueTask(Task.Delay(50));
        ctx.Start();
        var node = new Node(ctx, "syncctx_test");

        var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var disposeDone = new ManualResetEventSlim();
        var disposeThread = new Thread(() =>
        {
            SynchronizationContext.SetSynchronizationContext(new NonPumpingSynchronizationContext());
            try
            {
                pub.Dispose();
                node.Dispose();
            }
            finally
            {
                disposeDone.Set();
            }
        });
        disposeThread.Start();

        Assert.True(disposeDone.Wait(TimeSpan.FromSeconds(5)),
            "Dispose must complete within timeout on non-pumping SyncContext");
    }

    [Fact]
    public void ManualGateでadvertise_delay中にDisposeが完了する()
    {
        var advertiseGate = new ManualResetEventSlim();
        Exception? disposeError = null;

        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.SedpAdvertiseDelay = () =>
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ThreadPool.QueueUserWorkItem(_ =>
            {
                advertiseGate.Wait();
                tcs.SetResult();
            });
            return new ValueTask(tcs.Task);
        };
        ctx.Start();
        var node = new Node(ctx, "manual_gate_test");

        var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var disposeDone = new ManualResetEventSlim();
        var disposeThread = new Thread(() =>
        {
            try
            {
                pub.Dispose();
                node.Dispose();
            }
            catch (Exception ex)
            {
                disposeError = ex;
            }
            finally
            {
                disposeDone.Set();
            }
        });
        disposeThread.Start();

        try
        {
            Assert.False(disposeDone.Wait(TimeSpan.FromMilliseconds(200)),
                "Dispose must be blocked while advertise gate is closed");

            advertiseGate.Set();
            Assert.True(disposeDone.Wait(TimeSpan.FromSeconds(5)),
                "Dispose must complete after advertise gate is released");

            if (disposeError is not null)
                Assert.Null(disposeError);

            Assert.True(node.IsDisposed);
        }
        finally
        {
            advertiseGate.Set();
            Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)),
                "Dispose thread must join after completion");
        }
    }

    [Fact]
    public void NonPumpingSyncContext上でadvertise_delay中にDisposeが完了する()
    {
        var advertiseGate = new ManualResetEventSlim();
        Exception? disposeError = null;

        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.SedpAdvertiseDelay = () =>
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ThreadPool.QueueUserWorkItem(_ =>
            {
                advertiseGate.Wait();
                tcs.SetResult();
            });
            return new ValueTask(tcs.Task);
        };
        ctx.Start();
        var node = new Node(ctx, "nonpump_gate_test");

        var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var disposeDone = new ManualResetEventSlim();
        var disposeThread = new Thread(() =>
        {
            SynchronizationContext.SetSynchronizationContext(new NonPumpingSynchronizationContext());
            try
            {
                pub.Dispose();
                node.Dispose();
            }
            catch (Exception ex)
            {
                disposeError = ex;
            }
            finally
            {
                disposeDone.Set();
            }
        });
        disposeThread.Start();

        try
        {
            Assert.False(disposeDone.Wait(TimeSpan.FromMilliseconds(200)),
                "Dispose must be blocked while advertise gate is closed");

            advertiseGate.Set();
            Assert.True(disposeDone.Wait(TimeSpan.FromSeconds(5)),
                "Dispose must complete after advertise gate is released");

            if (disposeError is not null)
                Assert.Null(disposeError);

            Assert.True(node.IsDisposed);
        }
        finally
        {
            advertiseGate.Set();
            Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)),
                "Dispose thread must join after completion");
        }
    }

    [Fact]
    public void Barrierで同期した競合Disposeでevent_logの順序が正しい()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "race_barrier_test");

        var recordedEvents = new ConcurrentQueue<string>();
        node.TestEventRecorder = ev => recordedEvents.Enqueue(ev);

        using var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var barrier = new Barrier(3);
        Exception? pubDisposeError = null;
        Exception? nodeDisposeError = null;

        var pubDisposeThread = new Thread(() =>
        {
            try
            {
                barrier.SignalAndWait();
                pub.Dispose();
            }
            catch (Exception ex) { pubDisposeError = ex; }
        });

        var nodeDisposeThread = new Thread(() =>
        {
            try
            {
                barrier.SignalAndWait();
                node.Dispose();
            }
            catch (Exception ex) { nodeDisposeError = ex; }
        });

        pubDisposeThread.Start();
        nodeDisposeThread.Start();

        barrier.SignalAndWait();

        try
        {
            Assert.True(pubDisposeThread.Join(TimeSpan.FromSeconds(5)),
                "Publisher dispose thread must complete");
            Assert.True(nodeDisposeThread.Join(TimeSpan.FromSeconds(5)),
                "Node dispose thread must complete");
        }
        finally
        {
            if (pubDisposeThread.IsAlive) pubDisposeThread.Join(TimeSpan.FromSeconds(3));
            if (nodeDisposeThread.IsAlive) nodeDisposeThread.Join(TimeSpan.FromSeconds(3));
        }

        Assert.Null(pubDisposeError);
        Assert.Null(nodeDisposeError);
        Assert.True(node.IsDisposed);

        var eventsArray = recordedEvents.ToArray();
        var beforeUnregIdx = Array.IndexOf(eventsArray, "BeforeReceiverUnregisterWriter");
        var beforeSedpIdx = Array.IndexOf(eventsArray, "BeforeSedpUnregisterWriter");
        var afterSedpIdx = Array.IndexOf(eventsArray, "AfterSedpUnregisterWriter");

        Assert.True(beforeUnregIdx >= 0,
            $"BeforeReceiverUnregisterWriter not found in [{string.Join(", ", eventsArray)}]");
        Assert.True(beforeSedpIdx >= 0,
            $"BeforeSedpUnregisterWriter not found in [{string.Join(", ", eventsArray)}]");
        Assert.True(afterSedpIdx >= 0,
            $"AfterSedpUnregisterWriter not found in [{string.Join(", ", eventsArray)}]");
        Assert.True(beforeUnregIdx < beforeSedpIdx,
            $"receiver unregister must precede SEDP unregister; order: {string.Join(" -> ", eventsArray)}");
    }

    [Fact]
    public void 並行Node_Disposeで二回目が一回目の完了を待つ()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "race_test");

        var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        var sub = node.CreateSubscription<StringMessage>(
            "other", StringMessageSerializer.Instance, _ => { });

        var barrier = new Barrier(3);
        var disposals = new List<Thread>();

        for (int i = 0; i < 2; i++)
        {
            var t = new Thread(() =>
            {
                barrier.SignalAndWait();
                node.Dispose();
            });
            disposals.Add(t);
            t.Start();
        }

        barrier.SignalAndWait();
        foreach (var t in disposals)
        {
            Assert.True(t.Join(TimeSpan.FromSeconds(5)),
                "Dispose thread must complete");
        }

        Assert.True(node.IsDisposed);
    }

    [Fact]
    public void Context_Dispose経由のNode_Disposeが並行Dispose完了を待つ()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "ctx_race_test");

        var pub = node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var nodeDisposeDone = new ManualResetEventSlim();
        var nodeDisposeThread = new Thread(() =>
        {
            node.Dispose();
            nodeDisposeDone.Set();
        });

        var ctxDisposeThread = new Thread(() =>
        {
            ctx.Dispose();
        });

        nodeDisposeThread.Start();
        Thread.Sleep(50);
        ctxDisposeThread.Start();

        try
        {
            Assert.True(nodeDisposeDone.Wait(TimeSpan.FromSeconds(5)),
                "Node.Dispose must complete");
            Assert.True(ctxDisposeThread.Join(TimeSpan.FromSeconds(5)),
                "Context.Dispose thread must complete after Node dispose");
            Assert.True(node.IsDisposed);
            Assert.True(ctx.IsDisposed);
        }
        finally
        {
            if (nodeDisposeThread.IsAlive) nodeDisposeThread.Join(TimeSpan.FromSeconds(3));
            if (ctxDisposeThread.IsAlive) ctxDisposeThread.Join(TimeSpan.FromSeconds(3));
            if (!ctx.IsDisposed) ctx.Dispose();
        }
    }

    private sealed class NonPumpingSynchronizationContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object? state)
        {
        }
    }

    private sealed class CapturingSynchronizationContext : SynchronizationContext
    {
        public int PostCallCount;
        public override void Post(SendOrPostCallback d, object? state)
        {
            Interlocked.Increment(ref PostCallCount);
            ThreadPool.QueueUserWorkItem(_ => d(state));
        }
    }

    // ======== Fix 2: Dispose cleanup continuation ========

    [Fact]
    public void Disposeで先頭wrapper例外でも後続wrapperが処理される()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "cleanup_test");

        using var pub1 = node.CreatePublisher<StringMessage>(
            "topic1", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var pub2 = node.CreatePublisher<StringMessage>(
            "topic2", StringMessageSerializer.Instance, StringMessage.DdsTypeName);

        var wrappersField = typeof(Node).GetField("_trackedWrappers",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var wrappers = (List<IDisposable>)wrappersField.GetValue(node)!;
        wrappers.Insert(0, new ThrowingDisposable());
        int originalCount = wrappers.Count;

        Exception? disposeEx = null;
        try { node.Dispose(); }
        catch (Exception ex) { disposeEx = ex; }

        Assert.NotNull(disposeEx);
        Assert.IsType<InvalidOperationException>(disposeEx);
        Assert.True(node.IsDisposed);

        var remaining = (List<IDisposable>)wrappersField.GetValue(node)!;
        Assert.Empty(remaining);
    }

    [Fact]
    public void Disposeで複数endpointのcleanupがすべて実行される()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "multi_endpoint_cleanup_test");

        using var pub1 = node.CreatePublisher<StringMessage>(
            "topic1", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var pub2 = node.CreatePublisher<StringMessage>(
            "topic2", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var sub1 = node.CreateSubscription<StringMessage>(
            "topic3", StringMessageSerializer.Instance, _ => { });

        var snapshotBefore = node.Snapshot();
        Assert.Equal(2, snapshotBefore.Writers.Length);
        Assert.Single(snapshotBefore.Readers);

        Exception? disposeEx = null;
        try { node.Dispose(); }
        catch (Exception ex) { disposeEx = ex; }

        Assert.True(node.IsDisposed);

        var snapshotAfter = node.Snapshot();
        Assert.Empty(snapshotAfter.Writers);
        Assert.Empty(snapshotAfter.Readers);
    }

    // ======== Fix 3: ServiceClient lifecycle ========

    [Fact]
    public async Task ServiceClient_Dispose後にObjectDisposedException()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_lifecycle");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var client = node.CreateServiceClient(descriptor, svcName);

        var cts = new CancellationTokenSource();
        var waitTask = client.WaitForServiceAsync(TimeSpan.FromSeconds(10), cts.Token);

        await Task.Delay(50);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitTask);

        client.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.CallAsync(new StringMessage("after"), TimeSpan.FromMilliseconds(1)));
    }

    [Fact]
    public async Task ServiceClient_WaitForServiceAsync_Dispose済みならObjectDisposedException()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_wait_disposed");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var client = node.CreateServiceClient(descriptor, svcName);
        client.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.WaitForServiceAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task ServiceClient_WaitForServiceAsync_待機中にDisposeでObjectDisposedException()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        using var node = new Node(ctx, "svc_wait_dispose_race");

        var svcName = $"test_{System.Guid.NewGuid():N}";
        var descriptor = new ServiceDescriptor<StringMessage, StringMessage>(
            requestDdsTypeName: StringMessage.DdsTypeName,
            responseDdsTypeName: StringMessage.DdsTypeName,
            requestSerializer: StringMessageSerializer.Instance,
            responseSerializer: StringMessageSerializer.Instance);

        var client = node.CreateServiceClient(descriptor, svcName);

        var waitLoopEntered = new ManualResetEventSlim();
        client.WaitLoopEntered = () => waitLoopEntered.Set();

        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var delayInvoked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.WaitDelayAsync = (delay, ct) =>
        {
            delayInvoked.TrySetResult(true);
            return gate.Task.WaitAsync(ct);
        };

        var waitTask = Task.Run(async () =>
        {
            return await client.WaitForServiceAsync(TimeSpan.FromSeconds(30));
        });

        try
        {
            Assert.True(waitLoopEntered.Wait(TimeSpan.FromSeconds(5)),
                "WaitForServiceAsync should enter the wait loop");
            Assert.True(await delayInvoked.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                "WaitDelayAsync should be invoked");

            client.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => waitTask.WaitAsync(TimeSpan.FromSeconds(2)));
        }
        finally
        {
            gate.TrySetResult(true);
        }
    }

    [Theory]
    [InlineData("BeforeWriterStop")]
    [InlineData("BeforeWriterUnregister")]
    [InlineData("BeforeWriterDispose")]
    [InlineData("BeforeReaderStop")]
    [InlineData("BeforeReaderUnregister")]
    [InlineData("BeforeReaderDispose")]
    public void Node_Dispose_各cleanup操作のfault_injection_で後続cleanupとUnregisterNodeが継続する(string faultEvent)
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var node = new Node(ctx, "cleanup_fault_" + faultEvent);

        node.CreatePublisher<StringMessage>("topic_a", StringMessageSerializer.Instance);
        node.CreatePublisher<StringMessage>("topic_b", StringMessageSerializer.Instance);
        node.CreateSubscription<StringMessage>("topic_r", StringMessageSerializer.Instance, (_, _) => { });

        var wrappersField = typeof(Node).GetField("_trackedWrappers",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var wrappers = (List<IDisposable>)wrappersField.GetValue(node)!;
        wrappers.Clear();

        var events = new ConcurrentBag<string>();
        node.TestEventRecorder = eventName => events.Add(eventName);

        var faultInjected = false;
        node.EndpointCleanupFaultInjector = eventName =>
        {
            if (!faultInjected && eventName == faultEvent)
            {
                faultInjected = true;
                throw new InvalidOperationException($"injected fault at {faultEvent}");
            }
        };

        Exception? caughtEx = null;
        try { node.Dispose(); }
        catch (Exception ex) { caughtEx = ex; }

        Assert.NotNull(caughtEx);
        Assert.IsType<InvalidOperationException>(caughtEx);
        Assert.Equal($"injected fault at {faultEvent}", caughtEx.Message);
        Assert.True(node.IsDisposed);

        var eventList = events.ToList();
        eventList.Should().Contain("BeforeUnregisterNode",
            $"Context.UnregisterNode must be reached even after fault at {faultEvent}");
        eventList.Should().Contain("AfterUnregisterNode",
            $"Context.UnregisterNode must complete even after fault at {faultEvent}");

        var writerStopCount = eventList.Count(e => e == "BeforeWriterStop");
        writerStopCount.Should().Be(2, "both writers must attempt Stop");

        var writerUnregisterCount = eventList.Count(e => e == "BeforeWriterUnregister");
        writerUnregisterCount.Should().Be(2, "both writers must attempt Unregister");

        var writerDisposeCount = eventList.Count(e => e == "BeforeWriterDispose");
        writerDisposeCount.Should().Be(2, "both writers must attempt Dispose");

        eventList.Should().Contain("BeforeReaderStop", "reader Stop must be attempted");
        eventList.Should().Contain("BeforeReaderUnregister", "reader Unregister must be attempted");
        eventList.Should().Contain("BeforeReaderDispose", "reader Dispose must be attempted");
    }

    private static int GetPendingRegistrationsField(Node node)
    {
        var field = typeof(Node).GetField("_pendingRegistrations",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return (int)field!.GetValue(node)!;
    }

    private sealed class ThrowingDisposable : IDisposable
    {
        public void Dispose() => throw new InvalidOperationException("test throw");
    }

    private sealed class SilentTransport : IRtpsTransport
    {
        public Locator LocalLocator => Locator.FromUdpV4(IPAddress.Loopback, 7411);
        public event Action<ReadOnlyMemory<byte>, Locator>? Received { add { } remove { } }
        public ValueTask SendAsync(ReadOnlyMemory<byte> packet, Locator destination, CancellationToken cancellationToken = default) => default;
        public void Start() { }
        public void Stop() { }
        public void Dispose() { }
    }
}
