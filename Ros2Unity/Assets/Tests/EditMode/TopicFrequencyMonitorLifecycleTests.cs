using System;
using System.Threading;
using NUnit.Framework;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Dds;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Diagnostics;
using ROSettaDDS.Rtps;
using ROSettaDDS.Rtps.Reader;

using Guid = ROSettaDDS.Common.Guid;

namespace ROSettaDDS.UnityVerification.Tests
{
    public sealed class TopicFrequencyMonitorLifecycleTests
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

        [Test]
        public void CreateFrequencyMonitor_GetStatistics_空状態()
        {
            using var context = CreateContext();
            using var node = new Node(context, "unity_fm_test");
            using var diag = node.CreateTopicDiagnostics();

            var prefix = Prefix(70);
            context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
            context.DiscoveryDb.UpsertEndpoint(
                Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/unity_fm_test"), DateTime.UtcNow);

            using var monitor = diag.CreateFrequencyMonitor("/unity_fm_test");
            var stats = monitor.GetStatistics();

            Assert.AreEqual(0, stats.SampleCount);
            Assert.IsFalse(stats.HasData);
        }

        [Test]
        public void CreateFrequencyMonitor_Dispose_安全()
        {
            using var context = CreateContext();
            using var node = new Node(context, "unity_fm_disp");
            using var diag = node.CreateTopicDiagnostics();

            var prefix = Prefix(71);
            context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
            context.DiscoveryDb.UpsertEndpoint(
                Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/unity_fm_disp"), DateTime.UtcNow);

            var monitor = diag.CreateFrequencyMonitor("/unity_fm_disp");
            monitor.Dispose();
            monitor.Dispose();
        }

        [Test]
        public void Node_Dispose_でmonitor連鎖Dispose()
        {
            using var context = CreateContext();
            var node = new Node(context, "unity_fm_chain");
            using var diag = node.CreateTopicDiagnostics();

            var prefix = Prefix(72);
            context.DiscoveryDb.UpsertParticipant(Participant(prefix), DateTime.UtcNow);
            context.DiscoveryDb.UpsertEndpoint(
                Endpoint(prefix, EndpointKind.Writer, 0x10, "rt/unity_fm_chain"), DateTime.UtcNow);

            var monitor = diag.CreateFrequencyMonitor("/unity_fm_chain");
            node.Dispose();

            Assert.Throws<ObjectDisposedException>(() => monitor.GetStatistics());
        }

        // ======== Non-empty statistics via OnPayload production path ========

        [Test]
        public void OnPayload経路で非空統計が生成される()
        {
            var clock = new UnityMockClock();
            var opts = new TopicFrequencyOptions { WindowSize = 10 };
            using var monitor = new TopicFrequencyMonitor(opts, clock);

            var reader = new UnityTestUserReader(new EntityId(1, EntityKind.UserDefinedReaderNoKey));
            using var raw = new RawSubscription(
                "t", default, reader,
                monitor.OnPayload,
                autoStart: false);

            // SimulatePayload → RawSubscription callback → monitor.OnPayload → clock.GetTimestamp in lock
            for (int i = 0; i < 5; i++)
            {
                clock.Advance(1_000_000); // +100ms per step
                reader.SimulatePayload(ReadOnlyMemory<byte>.Empty, default);
            }

            var stats = monitor.GetStatistics();
            Assert.AreEqual(5, stats.SampleCount);
            Assert.IsTrue(stats.HasData);
            Assert.Greater(stats.RateHz, 0);
            Assert.Greater(stats.WindowDuration, TimeSpan.Zero);
        }

        private sealed class UnityMockClock : IClock
        {
            private long _now;
            public long GetTimestamp() => Interlocked.Read(ref _now);
            public TimeSpan GetElapsedTime(long startingTimestamp, long endingTimestamp)
            {
                if (endingTimestamp < startingTimestamp)
                    return TimeSpan.Zero;
                decimal delta = (decimal)endingTimestamp - (decimal)startingTimestamp;
                if (delta <= 0)
                    return TimeSpan.Zero;
                return TimeSpan.FromTicks((long)delta);
            }
            public long Advance(long ticks) => Interlocked.Add(ref _now, ticks);
        }

        private sealed class UnityTestUserReader : IUserReader
        {
            public UnityTestUserReader(EntityId readerEntityId)
            {
                ReaderEntityId = readerEntityId;
                Guid = new Guid(GuidPrefix.Unknown, readerEntityId);
            }
            public EntityId ReaderEntityId { get; }
            public Guid Guid { get; }
            public IRtpsSubmessageHandler Handler =>
                throw new NotSupportedException("UnityTestUserReader does not support Handler");
            public event Action<ReadOnlyMemory<byte>, GuidPrefix>? PayloadReceived;
            public int MatchedWriterCount => 0;
            public SubscriptionMatchedStatus SubscriptionMatchedStatus => default;
            public RtpsReaderDiagnostics Diagnostics =>
                throw new NotSupportedException("UnityTestUserReader does not support Diagnostics");
            public void MatchWriter(Guid writerGuid, Locator? unicastReplyLocator) { }
            public void UnmatchWriter(Guid writerGuid) { }
            public void Start() { }
            public void Stop() { }
            public void Dispose() { }
            public void SimulatePayload(ReadOnlyMemory<byte> payload, GuidPrefix sourcePrefix)
                => PayloadReceived?.Invoke(payload, sourcePrefix);
        }
    }
}
