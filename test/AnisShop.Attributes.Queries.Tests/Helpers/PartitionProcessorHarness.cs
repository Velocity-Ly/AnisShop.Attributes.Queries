using System.Collections.Concurrent;
using AnisShop.Kafka.Sessions;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    // Runs this application's session handler inside a real partition worker, without a broker.
    // Session grouping, ordering, blocking and offset safety belong to AnisShop.Kafka.Sessions and
    // are covered by that package's own suite; what this proves is the wiring — that our handler
    // turns a session's raw messages into read-model state.
    public sealed class PartitionProcessorHarness : IAsyncDisposable
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        private readonly ConcurrentQueue<TopicPartitionOffset> _positions = new();
        private readonly PartitionSessionWorker _worker;
        private readonly CancellationTokenSource _cts = new();

        public PartitionProcessorHarness(
            KafkaPartitionLog log,
            Func<ProcessSessionMessagesEventArgs, Task> handler)
        {
            Log = log;

            var options = new KafkaSessionProcessorOptions
            {
                BootstrapServers = "unused",
                Topic = log.TopicPartition.Topic,
                ConsumerGroup = "tests",
                MaxConcurrentPartitions = 1,
                // Tight timings so a blocked batch is retried quickly instead of making tests wait
                // out the production backoff.
                BatchLingerMilliseconds = 5,
                RetryBackoffMilliseconds = 5,
                MaxRetryBackoffMilliseconds = 50,
            };

            _worker = new PartitionSessionWorker(
                log.TopicPartition,
                options,
                new SemaphoreSlim(options.MaxConcurrentPartitions),
                new SemaphoreSlim(options.MaxConcurrentSessions),
                handler,
                errorHandler: null,
                _positions.Enqueue,
                NullLogger.Instance);

            _worker.Start(_cts.Token);
        }

        public KafkaPartitionLog Log { get; }

        // The position the worker would have had the processor store: the offset of the next
        // message to read, so everything below it has been handled.
        public long StoredPosition =>
            _positions.IsEmpty ? OffsetWatermark.Unset : _positions.Max(position => position.Offset.Value);

        public void Enqueue(params ConsumeResult<string, byte[]>[] records)
        {
            foreach (var record in records)
                Assert.True(_worker.TryEnqueue(record), "Partition buffer unexpectedly full");
        }

        public Task WaitForPosition(long expected, TimeSpan? timeout = null) =>
            WaitUntil(() => StoredPosition >= expected,
                $"stored position to reach {expected} (last seen: {StoredPosition})", timeout);

        public async Task WaitUntil(Func<bool> condition, string description, TimeSpan? timeout = null)
        {
            var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);

            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                    return;

                await Task.Delay(10);
            }

            Assert.Fail($"Timed out waiting for {description}.");
        }

        // Gives the worker time to prove it does NOT do something, such as advance past a blocked
        // offset. Long enough to cover many retry cycles at the harness backoff.
        public Task Settle() => Task.Delay(300);

        public async ValueTask DisposeAsync()
        {
            await _worker.StopAsync();
            _cts.Dispose();
        }
    }
}
