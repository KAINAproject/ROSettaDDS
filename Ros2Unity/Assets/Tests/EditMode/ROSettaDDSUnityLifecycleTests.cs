using NUnit.Framework;
using ROSettaDDS.Common;
using ROSettaDDS.Common.Logging;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Diagnostics;

namespace ROSettaDDS.UnityVerification.Tests
{
    public sealed class ROSettaDDSUnityLifecycleTests
    {
        [Test]
        public void Node_TopicDiagnostics_Dispose_連鎖()
        {
            var options = new ContextOptions
            {
                Logger = NullLogger.Instance,
                LocalhostOnly = true,
                EnableAutomaticNetworkRecovery = false,
            };
            using var context = new Context(options);
            var node = new Node(context, "lifecycle_test");
            var diag = node.CreateTopicDiagnostics();

            diag.Dispose();
            node.Dispose();
        }

        [Test]
        public void Node_Dispose_後にContext_Dispose()
        {
            var options = new ContextOptions
            {
                Logger = NullLogger.Instance,
                LocalhostOnly = true,
                EnableAutomaticNetworkRecovery = false,
            };
            var context = new Context(options);
            var node = new Node(context, "lifecycle_context");
            node.Dispose();
            context.Dispose();
        }
    }
}
