using System.Text;
using KafkaFlow;
using ConsumedRecord = Confluent.Kafka.ConsumeResult<string, byte[]>;

namespace AnisShop.Attributes.Queries.Tests.FakeServices
{
    // What KafkaFlow's batching middleware hands to the next middleware: one IMessageContext per
    // consumed record, carrying the raw broker bytes because no deserializer middleware is
    // registered. Built from a KafkaPartitionLog record, so both Kafka listeners are exercised
    // against the same fixtures.
    public class FakeMessageContext : IMessageContext
    {
        public FakeMessageContext(ConsumedRecord record)
        {
            Message = new Message(
                record.Message.Key is null ? null : Encoding.UTF8.GetBytes(record.Message.Key),
                record.Message.Value);

            Headers = new MessageHeaders(record.Message.Headers);
            ConsumerContext = new FakeConsumerContext(record);
        }

        public Message Message { get; }

        public IMessageHeaders Headers { get; }

        public IConsumerContext ConsumerContext { get; }

        public IProducerContext ProducerContext => throw new NotSupportedException();

        public IDependencyResolver DependencyResolver => throw new NotSupportedException();

        public IDictionary<string, object> Items { get; } = new Dictionary<string, object>();

        public IReadOnlyCollection<string> Brokers { get; } = ["unused"];

        public IMessageContext SetMessage(object key, object value) => throw new NotSupportedException();
    }

    // Only the fields the projection actually reads are real — where a message came from, so a
    // failure can name it. The rest belongs to the consume loop, which is entirely the package's
    // job under this transport.
    public class FakeConsumerContext : IConsumerContext
    {
        public FakeConsumerContext(ConsumedRecord record)
        {
            Topic = record.Topic;
            Partition = record.Partition.Value;
            Offset = record.Offset.Value;
        }

        public string Topic { get; }

        public int Partition { get; }

        public long Offset { get; }

        public TopicPartitionOffset TopicPartitionOffset => new(Topic, Partition, Offset);

        public string ConsumerName => "tests";

        public string GroupId => "tests";

        public int WorkerId => 0;

        public CancellationToken WorkerStopped => CancellationToken.None;

        public DateTime MessageTimestamp => DateTime.UnixEpoch;

        public bool AutoMessageCompletion { get; set; } = true;

        public bool ShouldStoreOffset { get; set; } = true;

        public IDependencyResolver ConsumerDependencyResolver => throw new NotSupportedException();

        public IDependencyResolver WorkerDependencyResolver => throw new NotSupportedException();

        public Task<TopicPartitionOffset> Completion => throw new NotSupportedException();

        public void Complete() => throw new NotSupportedException();

        public IOffsetsWatermark GetOffsetsWatermark() => throw new NotSupportedException();

        public void Pause() => throw new NotSupportedException();

        public void Pause(IReadOnlyList<TopicPartition> topicPartitions) => throw new NotSupportedException();

        public void Resume() => throw new NotSupportedException();

        public void Resume(IReadOnlyList<TopicPartition> topicPartitions) => throw new NotSupportedException();
    }
}
