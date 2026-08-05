using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace AnisShop.Kafka.Sessions.Tests.Fakes
{
    // Drives a real PartitionSessionWorker without a broker. The processor's only jobs are to hand
    // records to the right worker and to store the positions it reports, so both are replaced here
    // by TryEnqueue and a queue — everything that decides session grouping, ordering, blocking and
    // offset safety is the production class.
    public sealed class PartitionWorkerHarness : IAsyncDisposable
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        private readonly ConcurrentQueue<TopicPartitionOffset> _positions = new();
        private readonly PartitionSessionWorker _worker;
        private readonly CancellationTokenSource _cts = new();

        // startImmediately: false fills the buffer before the worker ever looks at it, which is the
        // only way to guarantee a given set of records lands in one batch.
        public PartitionWorkerHarness(
            PartitionLog log,
            int maxConcurrentSessions = 100,
            int maxMessagesPerSession = 100,
            int partitionBufferSize = 4000,
            bool startImmediately = true)
        {
            Log = log;
            Recorder = new SessionRecorder();

            var options = new KafkaSessionProcessorOptions
            {
                BootstrapServers = "unused",
                Topic = log.TopicPartition.Topic,
                ConsumerGroup = "tests",
                MaxConcurrentPartitions = 1,
                MaxConcurrentSessions = maxConcurrentSessions,
                MaxMessagesPerSession = maxMessagesPerSession,
                PartitionBufferSize = partitionBufferSize,
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
                new SemaphoreSlim(maxConcurrentSessions),
                Recorder.HandleAsync,
                Recorder.OnErrorAsync,
                _positions.Enqueue,
                NullLogger.Instance);

            if (startImmediately)
                Start();
        }

        public void Start() => _worker.Start(_cts.Token);

        public PartitionLog Log { get; }

        public SessionRecorder Recorder { get; }

        // The position the worker would have had the processor store: the offset of the next
        // message to read, so everything below it has been handled.
        public long StoredPosition =>
            _positions.IsEmpty ? OffsetWatermark.Unset : _positions.Max(position => position.Offset.Value);

        public void Enqueue(params ConsumeResult<string, byte[]>[] records)
        {
            foreach (var record in records)
                Assert.True(_worker.TryEnqueue(record), "Partition buffer unexpectedly full");
        }

        // The processor's backpressure signal: a false here is what makes it pause the partition on
        // the broker instead of buffering without limit.
        public bool TryEnqueue(ConsumeResult<string, byte[]> record) => _worker.TryEnqueue(record);

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
