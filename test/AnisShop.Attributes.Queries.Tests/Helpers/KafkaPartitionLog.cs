using System.Text;
using System.Text.Json;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using Confluent.Kafka;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    // Stands in for one Kafka partition: records get monotonically increasing offsets in append
    // order, exactly as a broker would hand them out. Tests interleave streams by choosing that
    // append order, which is the whole point — a partition carries many streams mixed together.
    public class KafkaPartitionLog
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };

        private readonly string _topic;
        private readonly int _partition;
        private long _nextOffset;

        public KafkaPartitionLog(string topic = "attributes-events", int partition = 0)
        {
            _topic = topic;
            _partition = partition;
        }

        public TopicPartition TopicPartition => new(_topic, new Partition(_partition));

        public long NextOffset => _nextOffset;

        public ConsumeResult<string, byte[]> Append(EventBase @event) =>
            Build(
                key: @event.AggregateId.ToString(),
                typeName: @event.GetType().Name,
                body: JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), JsonOptions));

        public ConsumeResult<string, byte[]>[] Append(IEnumerable<EventBase> events) =>
            [.. events.Select(Append)];

        // Interleaves the given streams round-robin, the way a real partition sees concurrent
        // producers: A1, B1, C1, A2, B2, C2, ...
        public ConsumeResult<string, byte[]>[] AppendInterleaved(params IReadOnlyList<EventBase>[] streams)
        {
            var records = new List<ConsumeResult<string, byte[]>>();
            var longest = streams.Max(stream => stream.Count);

            for (var index = 0; index < longest; index++)
            {
                foreach (var stream in streams.Where(stream => index < stream.Count))
                    records.Add(Append(stream[index]));
            }

            return [.. records];
        }

        public ConsumeResult<string, byte[]> AppendUnknownType(Guid aggregateId) =>
            Build(aggregateId.ToString(), "NotAnEventTypeWeKnow", Encoding.UTF8.GetBytes("{}"));

        // Another producer sharing the topic — a health probe or smoke test. It has a key and a body
        // but no event-type header at all, so nothing marks it as one of ours.
        public ConsumeResult<string, byte[]> AppendForeign(string key, string body) =>
            BuildRecord(key, new Headers(), Encoding.UTF8.GetBytes(body));

        private ConsumeResult<string, byte[]> Build(string key, string typeName, byte[] body)
        {
            var headers = new Headers { { KafkaEventDeserializer.TypeHeader, Encoding.UTF8.GetBytes(typeName) } };
            return BuildRecord(key, headers, body);
        }

        private ConsumeResult<string, byte[]> BuildRecord(string key, Headers headers, byte[] body) =>
            new()
            {
                Topic = _topic,
                Partition = new Partition(_partition),
                Offset = new Offset(_nextOffset++),
                Message = new Message<string, byte[]>
                {
                    Key = key,
                    Value = body,
                    Headers = headers,
                },
            };
    }
}
