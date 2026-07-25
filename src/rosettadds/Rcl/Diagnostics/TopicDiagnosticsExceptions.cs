using System;

namespace ROSettaDDS.Rcl.Diagnostics
{
    public sealed class TopicNotFoundException : InvalidOperationException
    {
        public TopicNotFoundException(string topicName)
            : base($"Topic '{topicName}' was not found.")
        {
        }
    }

    public sealed class AmbiguousTopicTypeException : InvalidOperationException
    {
        public AmbiguousTopicTypeException(string topicName)
            : base($"Topic '{topicName}' has multiple DDS types and cannot be uniquely selected.")
        {
        }
    }
}
