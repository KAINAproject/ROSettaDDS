using System;
using NUnit.Framework;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Dds;
using ROSettaDDS.Discovery;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Diagnostics;

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
    }
}
