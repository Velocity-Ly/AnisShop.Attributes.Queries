using System.Text;
using Confluent.Kafka;

namespace AnisShop.Kafka.Sessions.Tests.Fakes
{
    // Stands in for one Kafka partition: records get monotonically increasing offsets in append
    // order, exactly as a broker would hand them out. Tests interleave sessions by choosing that
    // append order, which is the whole point — a partition carries many sessions mixed together.
    //
    // The body is an opaque string. The package never reads it, which is the point of handing raw
    // records to the consumer.
    public sealed class PartitionLog
    {
        private readonly string _topic;
        private readonly int _partition;
        private long _nextOffset;

        public PartitionLog(string topic = "session-messages", int partition = 0)
        {
            _topic = topic;
            _partition = partition;
        }

        public TopicPartition TopicPartition => new(_topic, new Partition(_partition));

        public long NextOffset => _nextOffset;

        public ConsumeResult<string, byte[]> Append(string sessionId, string payload) =>
            Build(sessionId, payload);

        public ConsumeResult<string, byte[]>[] Append(string sessionId, params string[] payloads) =>
            [.. payloads.Select(payload => Build(sessionId, payload))];

        // The sender that forgot to set a key. Kafka gives such messages no session and no ordering.
        public ConsumeResult<string, byte[]> AppendWithoutKey(string payload) => Build(null, payload);

        // Interleaves the given sessions round-robin, the way a real partition sees concurrent
        // senders: A1, B1, C1, A2, B2, C2, ...
        public ConsumeResult<string, byte[]>[] AppendInterleaved(params (string SessionId, string[] Payloads)[] sessions)
        {
            var records = new List<ConsumeResult<string, byte[]>>();
            var longest = sessions.Max(session => session.Payloads.Length);

            for (var index = 0; index < longest; index++)
            {
                foreach (var session in sessions.Where(session => index < session.Payloads.Length))
                    records.Add(Build(session.SessionId, session.Payloads[index]));
            }

            return [.. records];
        }

        private ConsumeResult<string, byte[]> Build(string? key, string payload) =>
            new()
            {
                Topic = _topic,
                Partition = new Partition(_partition),
                Offset = new Offset(_nextOffset++),
                Message = new Message<string, byte[]>
                {
                    Key = key!,
                    Value = Encoding.UTF8.GetBytes(payload),
                },
            };
    }
}
