using ROSettaDDS.Common;
using ROSettaDDS.Discovery;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.Tests.Discovery;

public class DiscoveryDbTests
{
    private static GuidPrefix Prefix(byte id)
        => GuidPrefix.Create(VendorId.ROSettaDDS, id, (uint)(0x1000 + id), (ushort)(0x2000 + id));

    private static ParticipantData Participant(GuidPrefix prefix, double leaseSeconds = 20)
        => new()
        {
            Guid = new Guid(prefix, EntityId.Participant),
            LeaseDuration = Duration.FromSeconds(leaseSeconds),
        };

    private static DiscoveredEndpointData Endpoint(
        GuidPrefix prefix,
        EndpointKind kind,
        uint entityKey,
        string topic = "rt/chatter",
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

    [Fact]
    public void ExpireOldParticipants_は同じparticipant_prefixのendpointを削除してLostを通知する()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = GuidPrefix.Create(VendorId.ROSettaDDS, 0x01, 0x02, 0x03);
        var participantGuid = new Guid(prefix, EntityId.Participant);
        var writerGuid = new Guid(prefix, new EntityId(0x10u, EntityKind.UserDefinedWriterNoKey));
        var readerGuid = new Guid(prefix, new EntityId(0x11u, EntityKind.UserDefinedReaderNoKey));
        var events = new List<string>();

        db.WriterLost += endpoint => events.Add($"writer:{endpoint.Guid}");
        db.ReaderLost += endpoint => events.Add($"reader:{endpoint.Guid}");
        db.ParticipantLost += participant => events.Add($"participant:{participant.Guid}");

        db.UpsertParticipant(new ParticipantData
        {
            Guid = participantGuid,
            LeaseDuration = Duration.FromSeconds(1),
        }, now);
        db.UpsertEndpoint(new DiscoveredEndpointData
        {
            Kind = EndpointKind.Writer,
            EndpointGuid = writerGuid,
            ParticipantGuid = participantGuid,
            TopicName = "rt/chatter",
            TypeName = "std_msgs::msg::dds_::String_",
        }, now);
        db.UpsertEndpoint(new DiscoveredEndpointData
        {
            Kind = EndpointKind.Reader,
            EndpointGuid = readerGuid,
            ParticipantGuid = participantGuid,
            TopicName = "rt/chatter",
            TypeName = "std_msgs::msg::dds_::String_",
        }, now);

        db.ExpireOldParticipants(now + TimeSpan.FromSeconds(2));

        db.Count.Should().Be(0);
        db.WriterCount.Should().Be(0);
        db.ReaderCount.Should().Be(0);
        events.Should().Equal(
            $"writer:{writerGuid}",
            $"reader:{readerGuid}",
            $"participant:{participantGuid}");
    }

    [Fact]
    public void participant上限到達後の新規participantは保持せず既存更新は許可する()
    {
        var db = new DiscoveryDb(new DiscoveryLimits(maxRemoteParticipants: 1));
        var now = DateTime.UtcNow;
        var first = Participant(Prefix(1));
        var second = Participant(Prefix(2));

        db.UpsertParticipant(first, now);
        db.UpsertParticipant(second, now);
        db.Count.Should().Be(1);
        db.Snapshot()[0].Guid.Should().Be(first.Guid);

        first.EntityName = "updated";
        db.UpsertParticipant(first, now.AddSeconds(1));

        db.Count.Should().Be(1);
        db.Snapshot()[0].Data.EntityName.Should().Be("updated");
    }

    [Fact]
    public void 未知participantに属するendpointは拒否する()
    {
        var db = new DiscoveryDb();

        db.UpsertEndpoint(Endpoint(Prefix(1), EndpointKind.Writer, 0x10), DateTime.UtcNow);

        db.WriterCount.Should().Be(0);
    }

    [Fact]
    public void participant_guidとendpoint_guidのprefixが不一致ならendpointを拒否する()
    {
        var db = new DiscoveryDb();
        var participantPrefix = Prefix(1);
        var endpointPrefix = Prefix(2);
        db.UpsertParticipant(Participant(participantPrefix), DateTime.UtcNow);
        var endpoint = Endpoint(endpointPrefix, EndpointKind.Writer, 0x10);
        endpoint.ParticipantGuid = new Guid(participantPrefix, EntityId.Participant);

        db.UpsertEndpoint(endpoint, DateTime.UtcNow);

        db.WriterCount.Should().Be(0);
    }

    [Fact]
    public void writer上限到達後の新規writerは保持しない()
    {
        var db = new DiscoveryDb(new DiscoveryLimits(maxRemoteWriters: 1));
        var now = DateTime.UtcNow;
        var firstPrefix = Prefix(1);
        var secondPrefix = Prefix(2);
        db.UpsertParticipant(Participant(firstPrefix), now);
        db.UpsertParticipant(Participant(secondPrefix), now);

        db.UpsertEndpoint(Endpoint(firstPrefix, EndpointKind.Writer, 0x10), now);
        db.UpsertEndpoint(Endpoint(secondPrefix, EndpointKind.Writer, 0x11), now);

        db.WriterCount.Should().Be(1);
    }

    [Fact]
    public void participantあたりのendpoint上限到達後は新規endpointを保持しない()
    {
        var db = new DiscoveryDb(new DiscoveryLimits(maxRemoteEndpointsPerParticipant: 1));
        var now = DateTime.UtcNow;
        var prefix = Prefix(1);
        db.UpsertParticipant(Participant(prefix), now);

        db.UpsertEndpoint(Endpoint(prefix, EndpointKind.Writer, 0x10), now);
        db.UpsertEndpoint(Endpoint(prefix, EndpointKind.Reader, 0x11), now);

        db.WriterCount.Should().Be(1);
        db.ReaderCount.Should().Be(0);
    }

    [Fact]
    public void CreateEndpointSnapshot_はwriter_readerを値コピーし変更が影響しない()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = GuidPrefix.Create(VendorId.ROSettaDDS, 0x01, 0x02, 0x03);
        var participantGuid = new Guid(prefix, EntityId.Participant);
        var writerGuid = new Guid(prefix, new EntityId(0x10u, EntityKind.UserDefinedWriterNoKey));
        var readerGuid = new Guid(prefix, new EntityId(0x11u, EntityKind.UserDefinedReaderNoKey));

        db.UpsertParticipant(new ParticipantData
        {
            Guid = participantGuid,
            LeaseDuration = Duration.Infinite,
        }, now);
        db.UpsertEndpoint(new DiscoveredEndpointData
        {
            Kind = EndpointKind.Writer,
            EndpointGuid = writerGuid,
            ParticipantGuid = participantGuid,
            TopicName = "rt/chatter",
            TypeName = "std_msgs::msg::dds_::String_",
        }, now);
        db.UpsertEndpoint(new DiscoveredEndpointData
        {
            Kind = EndpointKind.Reader,
            EndpointGuid = readerGuid,
            ParticipantGuid = participantGuid,
            TopicName = "rt/chatter",
            TypeName = "std_msgs::msg::dds_::String_",
        }, now);

        var snapshot = db.CreateEndpointSnapshot();

        snapshot.Writers.Should().HaveCount(1);
        snapshot.Readers.Should().HaveCount(1);

        // 各 snapshot 取得で独立したクローンが生成される
        snapshot.Writers[0].TopicName = "rt/modified";

        var snapshot2 = db.CreateEndpointSnapshot();
        snapshot2.Writers[0].TopicName.Should().Be("rt/chatter");
    }

    [Fact]
    public void CreateEndpointSnapshot_は同じロック区間で一貫した集合を返す()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = GuidPrefix.Create(VendorId.ROSettaDDS, 0x01, 0x02, 0x03);
        var participantGuid = new Guid(prefix, EntityId.Participant);

        db.UpsertParticipant(new ParticipantData
        {
            Guid = participantGuid,
            LeaseDuration = Duration.Infinite,
        }, now);

        var writerGuid = new Guid(prefix, new EntityId(0x10u, EntityKind.UserDefinedWriterNoKey));
        db.UpsertEndpoint(new DiscoveredEndpointData
        {
            Kind = EndpointKind.Writer,
            EndpointGuid = writerGuid,
            ParticipantGuid = participantGuid,
            TopicName = "rt/foo",
            TypeName = "std_msgs::msg::dds_::String_",
        }, now);

        // Expire と CreateEndpointSnapshot の競合: 追加前または削除後の一貫した状態になる
        db.ExpireOldParticipants(now + TimeSpan.FromDays(1));
        var snapshot = db.CreateEndpointSnapshot();

        snapshot.Writers.Should().BeEmpty();
        snapshot.Readers.Should().BeEmpty();
    }

    [Fact]
    public void remote_participant_lease_durationは保持前にclampされる()
    {
        var db = new DiscoveryDb(new DiscoveryLimits(
            minRemoteParticipantLeaseSeconds: 1,
            maxRemoteParticipantLeaseSeconds: 2));
        var now = DateTime.UtcNow;
        var prefix = Prefix(1);

        db.UpsertParticipant(Participant(prefix, leaseSeconds: 100), now);

        db.Snapshot()[0].Data.LeaseDuration.ToTimeSpan()
            .Should().BeCloseTo(TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(1));

        db.UpsertParticipant(Participant(prefix, leaseSeconds: 0.1), now.AddSeconds(1));

        db.Snapshot()[0].Data.LeaseDuration.ToTimeSpan()
            .Should().BeCloseTo(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public void CreateEndpointSnapshot保持中はUpsertEndpointがDiscoveryDbの_lock待ちになる()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = Prefix(30);
        var participantGuid = new Guid(prefix, EntityId.Participant);
        var writerGuid = new Guid(prefix, new EntityId(0x10u, EntityKind.UserDefinedWriterNoKey));

        db.UpsertParticipant(Participant(prefix), now);

        var snapshotHoldsLock = new ManualResetEventSlim();
        var snapshotContinue = new ManualResetEventSlim();
        db.SnapshotLockAcquiredCallback = () =>
        {
            snapshotHoldsLock.Set();
            if (!snapshotContinue.Wait(TimeSpan.FromSeconds(5)))
                throw new TimeoutException("snapshot hold lock timed out");
        };

        var mutationReachedLockGate = new ManualResetEventSlim();
        var mutationAcquiredLock = new ManualResetEventSlim();
        var mutationCompleted = new ManualResetEventSlim();

        db.ExternalLockEnter = () => mutationReachedLockGate.Set();
        db.MutationLockAcquiredCallback = () => mutationAcquiredLock.Set();

        var snapshotDone = new ManualResetEventSlim();
        Exception? snapshotError = null;
        var snapshotThread = new Thread(() =>
        {
            try
            {
                db.CreateEndpointSnapshot();
            }
            catch (Exception ex)
            {
                snapshotError = ex;
            }
            finally
            {
                snapshotDone.Set();
            }
        });
        snapshotThread.Start();

        Assert.True(snapshotHoldsLock.Wait(TimeSpan.FromSeconds(5)),
            "snapshot must acquire _lock");

        Exception? mutationError = null;
        var mutationThread = new Thread(() =>
        {
            try
            {
                db.UpsertEndpoint(
                    new DiscoveredEndpointData
                    {
                        Kind = EndpointKind.Writer,
                        EndpointGuid = writerGuid,
                        ParticipantGuid = participantGuid,
                        TopicName = "rt/race",
                        TypeName = "std_msgs::msg::dds_::String_",
                    },
                    DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                mutationError = ex;
            }
            finally
            {
                mutationCompleted.Set();
            }
        });
        mutationThread.Start();

        Assert.True(mutationReachedLockGate.Wait(TimeSpan.FromSeconds(5)),
            "mutation must reach ExternalLockEnter");

        Assert.False(mutationCompleted.Wait(TimeSpan.FromMilliseconds(300)),
            "mutation must be blocked on _lock while snapshot holds it");
        Assert.False(mutationAcquiredLock.IsSet,
            "mutation must not acquire _lock while snapshot holds it");

        snapshotContinue.Set();
        Assert.True(snapshotDone.Wait(TimeSpan.FromSeconds(5)),
            "snapshot must complete after barrier released");

        if (snapshotError is not null)
            throw new Exception("snapshot thread failed", snapshotError);

        Assert.True(mutationCompleted.Wait(TimeSpan.FromSeconds(5)),
            "mutation must complete after snapshot releases _lock");
        Assert.True(mutationAcquiredLock.Wait(TimeSpan.FromSeconds(5)),
            "mutation must acquire _lock after snapshot releases it");

        Assert.True(mutationThread.Join(TimeSpan.FromSeconds(5)), "mutation thread must join");
        if (mutationError is not null)
            throw new Exception("mutation thread failed", mutationError);

        db.SnapshotLockAcquiredCallback = null;
        db.MutationLockAcquiredCallback = null;
        db.ExternalLockEnter = null;
    }

    [Fact]
    public void WriterDiscoveredと並行removeのhandler呼び出しはdeadlockしない()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = Prefix(1);
        var pGuid = new Guid(prefix, EntityId.Participant);
        var wGuid = new Guid(prefix, new EntityId(0x10, EntityKind.UserDefinedWriterNoKey));

        db.UpsertParticipant(new ParticipantData { Guid = pGuid, LeaseDuration = Duration.Infinite }, now);

        var received = new List<string>();
        var receivedLock = new object();

        var discoveredBlocked = new ManualResetEventSlim();
        var discoveredContinue = new ManualResetEventSlim();

        db.WriterDiscovered += _ =>
        {
            discoveredBlocked.Set();
            Assert.True(discoveredContinue.Wait(TimeSpan.FromSeconds(5)),
                "handler must unblock within timeout");
            lock (receivedLock) { received.Add("discovered"); }
        };
        db.WriterLost += _ => { lock (receivedLock) { received.Add("lost"); } };

        var t1 = new Thread(() =>
        {
            db.UpsertEndpoint(new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                EndpointGuid = wGuid,
                ParticipantGuid = pGuid,
                TopicName = "rt/test",
                TypeName = "TypeA",
            }, DateTime.UtcNow);
        });
        var t2 = new Thread(() =>
        {
            Assert.True(discoveredBlocked.Wait(TimeSpan.FromSeconds(5)),
                "handler must block first");
            db.TryRemoveEndpoint(EndpointKind.Writer, wGuid);
        });

        t1.Start();
        t2.Start();
        Assert.True(t2.Join(TimeSpan.FromSeconds(5)), "t2 must complete");
        discoveredContinue.Set();
        Assert.True(t1.Join(TimeSpan.FromSeconds(5)), "t1 must complete");

        List<string> snapshot;
        lock (receivedLock) { snapshot = new List<string>(received); }
        snapshot.Should().Contain("discovered");
        snapshot.Should().Contain("lost");
        snapshot.Should().HaveCount(2);
    }

    [Fact]
    public void CreateEndpointSnapshot保持中はTryRemoveEndpointがDiscoveryDbの_lock待ちになる()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = Prefix(31);
        var participantGuid = new Guid(prefix, EntityId.Participant);
        var writerGuid = new Guid(prefix, new EntityId(0x10u, EntityKind.UserDefinedWriterNoKey));

        db.UpsertParticipant(Participant(prefix), now);
        db.UpsertEndpoint(
            new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                EndpointGuid = writerGuid,
                ParticipantGuid = participantGuid,
                TopicName = "rt/remove_race",
                TypeName = "std_msgs::msg::dds_::String_",
            },
            now);

        var snapshotHoldsLock = new ManualResetEventSlim();
        var snapshotContinue = new ManualResetEventSlim();
        db.SnapshotLockAcquiredCallback = () =>
        {
            snapshotHoldsLock.Set();
            if (!snapshotContinue.Wait(TimeSpan.FromSeconds(5)))
                throw new TimeoutException("snapshot hold lock timed out");
        };

        var mutationReachedLockGate = new ManualResetEventSlim();
        var mutationAcquiredLock = new ManualResetEventSlim();
        var mutationCompleted = new ManualResetEventSlim();

        db.ExternalLockEnter = () => mutationReachedLockGate.Set();
        db.MutationLockAcquiredCallback = () => mutationAcquiredLock.Set();

        var snapshotDone = new ManualResetEventSlim();
        Exception? snapshotError = null;
        var snapshotThread = new Thread(() =>
        {
            try
            {
                db.CreateEndpointSnapshot();
            }
            catch (Exception ex)
            {
                snapshotError = ex;
            }
            finally
            {
                snapshotDone.Set();
            }
        });
        snapshotThread.Start();

        Assert.True(snapshotHoldsLock.Wait(TimeSpan.FromSeconds(5)),
            "snapshot must acquire _lock");

        Exception? mutationError = null;
        var mutationThread = new Thread(() =>
        {
            try
            {
                db.TryRemoveEndpoint(EndpointKind.Writer, writerGuid);
            }
            catch (Exception ex)
            {
                mutationError = ex;
            }
            finally
            {
                mutationCompleted.Set();
            }
        });
        mutationThread.Start();

        Assert.True(mutationReachedLockGate.Wait(TimeSpan.FromSeconds(5)),
            "mutation must reach ExternalLockEnter");

        Assert.False(mutationCompleted.Wait(TimeSpan.FromMilliseconds(300)),
            "mutation must be blocked on _lock while snapshot holds it");
        Assert.False(mutationAcquiredLock.IsSet,
            "mutation must not acquire _lock while snapshot holds it");

        snapshotContinue.Set();
        Assert.True(snapshotDone.Wait(TimeSpan.FromSeconds(5)),
            "snapshot must complete after barrier released");

        if (snapshotError is not null)
            throw new Exception("snapshot thread failed", snapshotError);

        Assert.True(mutationCompleted.Wait(TimeSpan.FromSeconds(5)),
            "mutation must complete after snapshot releases _lock");
        Assert.True(mutationAcquiredLock.Wait(TimeSpan.FromSeconds(5)),
            "mutation must acquire _lock after snapshot releases it");

        Assert.True(mutationThread.Join(TimeSpan.FromSeconds(5)), "mutation thread must join");
        if (mutationError is not null)
            throw new Exception("mutation thread failed", mutationError);

        db.SnapshotLockAcquiredCallback = null;
        db.MutationLockAcquiredCallback = null;
        db.ExternalLockEnter = null;
    }

    [Fact]
    public void イベントhandler内で他threadのmutationを待ってもdeadlockしない()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix = Prefix(1);
        var pGuid = new Guid(prefix, EntityId.Participant);
        var wGuid = new Guid(prefix, new EntityId(0x10, EntityKind.UserDefinedWriterNoKey));

        db.UpsertParticipant(new ParticipantData { Guid = pGuid, LeaseDuration = Duration.Infinite }, now);

        var handlerBlocked = new ManualResetEventSlim();
        var handlerContinue = new ManualResetEventSlim();

        db.WriterDiscovered += _ =>
        {
            handlerBlocked.Set();
            Assert.True(handlerContinue.Wait(TimeSpan.FromSeconds(5)),
                "handler must unblock within timeout (no deadlock)");
        };

        var mutationDone = new ManualResetEventSlim();

        var threadA = new Thread(() =>
        {
            db.UpsertEndpoint(new DiscoveredEndpointData
            {
                Kind = EndpointKind.Writer,
                EndpointGuid = wGuid,
                ParticipantGuid = pGuid,
                TopicName = "rt/t",
                TypeName = "T",
            }, DateTime.UtcNow);
        });

        var threadB = new Thread(() =>
        {
            Assert.True(handlerBlocked.Wait(TimeSpan.FromSeconds(5)),
                "handler must block first");
            // この mutation が deadlock せずに完了すること
            var prefix2 = Prefix(2);
            db.UpsertParticipant(new ParticipantData
            {
                Guid = new Guid(prefix2, EntityId.Participant),
                LeaseDuration = Duration.Infinite,
            }, DateTime.UtcNow);
            mutationDone.Set();
        });

        threadA.Start();
        threadB.Start();

        Assert.True(mutationDone.Wait(TimeSpan.FromSeconds(5)),
            "mutation in other thread must complete without deadlock");

        handlerContinue.Set();

        Assert.True(threadA.Join(TimeSpan.FromSeconds(5)), "threadA must join");
        Assert.True(threadB.Join(TimeSpan.FromSeconds(5)), "threadB must join");
    }

    [Fact]
    public void イベントはenqueue順にFIFOでdispatchされる()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefixes = Enumerable.Range(1, 5).Select(i => Prefix((byte)i)).ToArray();

        var receivedOrder = new List<GuidPrefix>();
        db.ParticipantDiscovered += p => receivedOrder.Add(p.Guid.Prefix);

        foreach (var prefix in prefixes)
        {
            db.UpsertParticipant(Participant(prefix), now);
        }

        receivedOrder.Should().Equal(prefixes);
    }

    [Fact]
    public void イベントhandler内でreentrantにmutationしてもdeadlockせず後続イベントもdispatchされる()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix1 = Prefix(1);
        var pGuid1 = new Guid(prefix1, EntityId.Participant);
        var prefix2 = Prefix(2);
        var pGuid2 = new Guid(prefix2, EntityId.Participant);
        var prefix3 = Prefix(3);
        var pGuid3 = new Guid(prefix3, EntityId.Participant);

        db.UpsertParticipant(new ParticipantData { Guid = pGuid1, LeaseDuration = Duration.Infinite }, now);

        var discoveredOrder = new List<GuidPrefix>();
        var handlerTriggered = new ManualResetEventSlim();

        db.ParticipantDiscovered += p =>
        {
            discoveredOrder.Add(p.Guid.Prefix);
            if (p.Guid.Prefix.Equals(prefix2) && !handlerTriggered.IsSet)
            {
                handlerTriggered.Set();
                // Handler 内でさらに別の participant を追加 → この中で TryDispatch が呼ばれる
                db.UpsertParticipant(new ParticipantData
                {
                    Guid = pGuid3,
                    LeaseDuration = Duration.Infinite,
                }, now.AddSeconds(1));
            }
        };

        var done = new ManualResetEventSlim();
        var thread = new Thread(() =>
        {
            db.UpsertParticipant(new ParticipantData
            {
                Guid = pGuid2,
                LeaseDuration = Duration.Infinite,
            }, now.AddSeconds(1));
            done.Set();
        });
        thread.Start();

        Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
            "mutation must complete without deadlock");
        Assert.True(thread.Join(TimeSpan.FromSeconds(5)), "thread must join");

        discoveredOrder.Should().Equal(prefix2, prefix3);
    }

    [Fact]
    public void 例外を投げるsubscriberがいる場合も他のsubscriberと後続イベントが実行される()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;
        var prefix1 = Prefix(1);
        var prefix2 = Prefix(2);

        var called = new List<string>();
        var callLock = new object();

        db.ParticipantDiscovered += p =>
        {
            lock (callLock) called.Add($"A:{p.Guid.Prefix}");
            throw new InvalidOperationException("handler A throws on purpose");
        };
        db.ParticipantDiscovered += p =>
        {
            lock (callLock) called.Add($"B:{p.Guid.Prefix}");
        };

        db.UpsertParticipant(Participant(prefix1), now);
        db.UpsertParticipant(Participant(prefix2), now);

        // RED: 現状は A(exception) のみ呼ばれ B は失われる (count = 2)
        // GREEN: 全 subscriber が個別 try/catch で呼ばれる (count = 4)
        lock (callLock)
        {
            called.Should().HaveCount(4,
                "both subscribers must be called for each event; expected 4 (A+B)×2, got {0}",
                string.Join(", ", called.Select(s => $"\"{s}\"")));
        }
    }

    [Fact]
    public void 並列enqueueでもhandlerは直列実行されFIFO完了順になる()
    {
        var db = new DiscoveryDb();
        var now = DateTime.UtcNow;

        var finishOrder = new List<int>();
        var finishLock = new object();
        var handlerStarted = new ManualResetEventSlim();

        db.ParticipantDiscovered += p =>
        {
            var id = int.Parse(p.Data.EntityName!);
            if (id == 1)
            {
                handlerStarted.Set();
                Thread.Sleep(500);
            }
            lock (finishLock) finishOrder.Add(id);
        };

        // Thread A: item 1 を enqueue → drain 開始 (handler が 500ms かかる)
        var threadA = new Thread(() =>
        {
            db.UpsertParticipant(new ParticipantData
            {
                Guid = new Guid(Prefix(1), EntityId.Participant),
                LeaseDuration = Duration.Infinite,
                EntityName = "1",
            }, now);
        });
        threadA.Start();

        // Thread A の handler が sleep に入るのを待つ
        Assert.True(handlerStarted.Wait(TimeSpan.FromSeconds(5)),
            "handler for item 1 must start");

        // この時点で Thread A の handler が 500ms sleep 中
        // Thread B: item 2 を enqueue
        var threadB = new Thread(() =>
        {
            db.UpsertParticipant(new ParticipantData
            {
                Guid = new Guid(Prefix(2), EntityId.Participant),
                LeaseDuration = Duration.Infinite,
                EntityName = "2",
            }, now);
        });
        threadB.Start();

        Assert.True(threadB.Join(TimeSpan.FromSeconds(10)), "threadB must complete");
        Assert.True(threadA.Join(TimeSpan.FromSeconds(10)), "threadA must complete");

        // RED: 現状は concurrent drain で item 2 の handler が先に完了し [2, 1] になる
        // GREEN: _draining を invoke 中も保持することで直列実行され [1, 2] になる
        lock (finishLock)
        {
            finishOrder.Should().Equal(1, 2);
        }
    }
}
