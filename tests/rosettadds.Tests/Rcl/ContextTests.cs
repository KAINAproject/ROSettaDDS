using System.Net.NetworkInformation;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rcl;
using ROSettaDDS.Msgs.Std;
using ROSettaDDS.Rtps;
using ROSettaDDS.Transport;
using Guid = ROSettaDDS.Common.Guid;
using Xunit;
using ROSettaDDS.Rcl.Naming;
using System.Runtime.ExceptionServices;

namespace ROSettaDDS.Tests.Rcl;

public class ContextTests
{
    [Fact]
    public void コンストラクタは_GuidPrefix_を生成する()
    {
        using var ctx = new Context(new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
        });

        Assert.NotEqual(default, ctx.GuidPrefix);
        Assert.Equal(new Guid(ctx.GuidPrefix, BuiltinEntityIds.Participant), ctx.Guid);
    }

    [Fact]
    public void コンストラクタは_options_を保持する()
    {
        var opts = new ContextOptions { DomainId = 42, LocalhostOnly = true, Logger = NullLogger.Instance };
        using var ctx = new Context(opts);
        Assert.Same(opts, ctx.Options);
    }

    [Fact]
    public void null_options_を渡すと_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new Context(null!));
    }

    [Fact]
    public void コンストラクタ完了時に_transport_4_種と_DiscoveryDb_が利用可能な状態になる()
    {
        using var ctx = new Context(new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
        });

        // Start 前は transport を内部で作っているが、外から見える形にしていないので
        // ResolvedParticipantId / DiscoveryDb が NotImplementedException にならないことだけ確認。
        // auto-probe が ID 0 で成功することがあるため、値自体は 0 以上であれば良い。
        Assert.True(ctx.ResolvedParticipantId >= 0);
        Assert.NotNull(ctx.DiscoveryDb);
    }

    [Fact]
    public void UserMulticast_と_UserUnicast_と_Destination_が_context_から取得できる()
    {
        using var ctx = new Context(new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
        });

        Assert.NotNull(ctx.UserMulticastTransport);
        Assert.NotNull(ctx.UserUnicastTransport);
        Assert.NotEqual(Locator.Invalid, ctx.UserMulticastDestination);
    }

    [Fact]
    public void Start_Stop_は_冪等()
    {
        using var ctx = new Context(new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
        });
        ctx.Start();
        ctx.Start();  // 二度呼んでも例外なし
        ctx.Stop();
        ctx.Stop();   // 二度呼んでも例外なし
    }

    [Fact]
    public void Dispose_後は_Start_が_ObjectDisposedException()
    {
        var ctx = new Context(new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
        });
        ctx.Dispose();
        Assert.Throws<ObjectDisposedException>(() => ctx.Start());
    }

    [Fact]
    public void Dispose_後は_Stop_が_ObjectDisposedException()
    {
        var ctx = new Context(new ContextOptions
        {
            LocalhostOnly = true,
            Logger = NullLogger.Instance,
        });
        ctx.Dispose();
        Assert.Throws<ObjectDisposedException>(() => ctx.Stop());
    }

    [Fact]
    public void Context_Dispose_時に_生存中の_Node_を_先に_Dispose_する()
    {
        var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        var node = new Node(ctx, "test");
        ctx.Dispose();
        Assert.Throws<ObjectDisposedException>(() => node.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName));
    }

    [Fact]
    public void 同一_Context_上の_2_Node_で_user_EntityId_重複なし()
    {
        using var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        using var node1 = new Node(ctx, "node1");
        using var node2 = new Node(ctx, "node2");

        // 各 Node 経由で publisher を作り、割り当てられる EntityId が重複しないことを確認
        using var pub1 = node1.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var pub2 = node2.CreatePublisher<StringMessage>(
            "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        // pub1 と pub2 の内部 EntityId が異なることを直接確認できないため、
        // Context.UserEntityIds が共有されており writer key が別々に進むことを検証
        var idA = ctx.UserEntityIds.AllocateWriter();
        var idB = ctx.UserEntityIds.AllocateWriter();
        Assert.NotEqual(idA, idB);
    }

    [Fact]
    public void 自動復旧は既定で通知を購読し_Disposeで解除する()
    {
        var source = new FakeNetworkChangeSource();
        var ctx = new Context(
            new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance },
            source);

        source.SubscriberCount.Should().Be(1);

        ctx.Dispose();

        source.SubscriberCount.Should().Be(0);
    }

    [Fact]
    public void 自動復旧を無効化すると通知を購読しない()
    {
        var source = new FakeNetworkChangeSource();
        using var ctx = new Context(
            new ContextOptions
            {
                LocalhostOnly = true,
                EnableAutomaticNetworkRecovery = false,
                Logger = NullLogger.Instance,
            },
            source);

        source.SubscriberCount.Should().Be(0);
    }

    [Fact]
    public async Task 復旧後も既存endpointとtransportを維持して再広告する()
    {
        var source = new FakeNetworkChangeSource();
        using var ctx = new Context(
            new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance },
            source);
        using var node = new Node(ctx, "recovery_test");
        var topicName = $"chatter_{System.Guid.NewGuid():N}";
        using var publisher = node.CreatePublisher<StringMessage>(
            topicName, StringMessageSerializer.Instance, StringMessage.DdsTypeName);
        using var subscription = node.CreateSubscription<StringMessage>(
            topicName, StringMessageSerializer.Instance, _ => { });
        ctx.Start();
        await WaitUntilAsync(
            () => ctx.PublishedPublicationStateCount > 0
                  && ctx.PublishedSubscriptionStateCount > 0,
            TimeSpan.FromSeconds(2));
        var multicastTransport = ctx.UserMulticastTransport;
        var publicationCount = ctx.PublishedPublicationStateCount;
        var subscriptionCount = ctx.PublishedSubscriptionStateCount;

        await ctx.RecoverNetworkAsync(CancellationToken.None);

        ctx.UserMulticastTransport.Should().BeSameAs(multicastTransport);
        ctx.PublishedPublicationStateCount.Should().BeGreaterThan(publicationCount);
        ctx.PublishedSubscriptionStateCount.Should().BeGreaterThan(subscriptionCount);
        await publisher.PublishAsync(new StringMessage("after recovery"));
        using var additionalPublisher = node.CreatePublisher<StringMessage>(
            "after_recovery", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("Condition was not satisfied within the timeout.");
            }
            await Task.Delay(10);
        }
    }

    private sealed class FakeNetworkChangeSource : INetworkChangeSource
    {
        private NetworkAddressChangedEventHandler? _networkAddressChanged;

        public int SubscriberCount { get; private set; }

        public event NetworkAddressChangedEventHandler? NetworkAddressChanged
        {
            add
            {
                _networkAddressChanged += value;
                SubscriberCount++;
            }
            remove
            {
                _networkAddressChanged -= value;
                SubscriberCount--;
            }
        }
    }

    // ======== Fix 1: RegisterNode/Dispose barrier ========

    [Fact]
    public void Disposeの_disposeInProgress到達後にRegisterNodeがObjectDisposedException()
    {
        var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });
        ctx.Start();
        var blocker = new Node(ctx, "blocker");

        var disposeInProgressReached = new ManualResetEventSlim();
        ctx.DisposeInProgressCallback = () => disposeInProgressReached.Set();

        var disposeThread = new Thread(() => ctx.Dispose());
        disposeThread.Start();

        Assert.True(disposeInProgressReached.Wait(TimeSpan.FromSeconds(5)),
            "Dispose should reach _disposeInProgress state");

        Exception? registerError = null;
        var lateNodeThread = new Thread(() =>
        {
            try { _ = new Node(ctx, "late"); }
            catch (Exception ex) { registerError = ex; }
        });
        lateNodeThread.Start();

        Assert.True(lateNodeThread.Join(TimeSpan.FromSeconds(5)),
            "Late RegisterNode thread must complete (no deadlock)");
        Assert.NotNull(registerError);
        Assert.IsType<ObjectDisposedException>(registerError);

        Assert.True(disposeThread.Join(TimeSpan.FromSeconds(5)));
        Assert.True(ctx.IsDisposed);
    }

    [Fact]
    public void 並行Context_Disposeで二回目が一回目の完了を待つ()
    {
        var ctx = new Context(new ContextOptions { LocalhostOnly = true, Logger = NullLogger.Instance });

        var barrier = new Barrier(3);
        var disposals = new Thread[2];

        for (int i = 0; i < 2; i++)
        {
            disposals[i] = new Thread(() =>
            {
                barrier.SignalAndWait();
                ctx.Dispose();
            });
            disposals[i].Start();
        }

        barrier.SignalAndWait();

        foreach (var t in disposals)
        {
            Assert.True(t.Join(TimeSpan.FromSeconds(5)),
                "Dispose thread must complete");
        }

        Assert.True(ctx.IsDisposed);
    }
}
