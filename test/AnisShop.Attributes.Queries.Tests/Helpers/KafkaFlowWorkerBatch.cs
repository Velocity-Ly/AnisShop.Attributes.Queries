using AnisShop.Attributes.Queries.Tests.FakeServices;
using Confluent.Kafka;
using KafkaFlow;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    // A worker's dispatch, assembled from partition records.
    //
    // KafkaFlow distributes by hashing the message key onto a fixed worker, so what one worker
    // collects is a hash bucket: several unrelated aggregates, possibly from several partitions,
    // in arrival order. That is deliberately the shape these tests build.
    public static class KafkaFlowWorkerBatch
    {
        public static IReadOnlyCollection<IMessageContext> Of(params ConsumeResult<string, byte[]>[] records) =>
            [.. records.Select(record => new FakeMessageContext(record))];
    }
}
