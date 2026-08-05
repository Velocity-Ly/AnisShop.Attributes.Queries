using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AnisShop.Kafka.Sessions;

/// <summary>
/// Consumes a Kafka topic as if its message keys were Service Bus session ids: messages of one
/// session are delivered in production order and never concurrently with themselves, while
/// different sessions are handled in parallel.
/// </summary>
/// <remarks>
/// Modelled on <c>ServiceBusSessionProcessor</c>: subscribe to
/// <see cref="ProcessSessionMessagesAsync"/>, optionally to <see cref="ProcessErrorAsync"/>, then
/// call <see cref="StartProcessingAsync"/>. The one difference is the handler signature — it
/// receives a *run* of messages from one session rather than a single message.
/// <para>
/// The processor owns the consumer and nothing else. It polls, routes each record to the worker
/// that owns its partition, applies per-partition backpressure, and stores the offsets those
/// workers report. Ordering and concurrency belong to <see cref="PartitionSessionWorker"/>.
/// </para>
/// </remarks>
public sealed class KafkaSessionProcessor : IAsyncDisposable
{
    private static readonly TimeSpan PollTimeout = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan RebalanceDrainTimeout = TimeSpan.FromSeconds(30);

    private readonly KafkaSessionProcessorOptions _options;
    private readonly Action<ConsumerConfig>? _configureConsumer;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<KafkaSessionProcessor> _logger;

    // The two concurrency gates, shared by every partition worker in this processor.
    private readonly SemaphoreSlim _partitionSlots;
    private readonly SemaphoreSlim _sessionSlots;

    // Both of these are only ever touched from the consume thread (the rebalance callbacks run
    // inside Consume), so they need no synchronisation of their own.
    private readonly Dictionary<TopicPartition, PartitionSessionWorker> _workers = [];
    private readonly Dictionary<TopicPartition, ConsumeResult<string, byte[]>> _pausedWithStash = [];

    // Workers run off-thread, so the positions they finish with are handed back through a queue
    // and stored by the consume thread — librdkafka's consumer is not thread-safe.
    private readonly ConcurrentQueue<TopicPartitionOffset> _readyPositions = new();

    private Func<ProcessSessionMessagesEventArgs, Task>? _messageHandler;
    private Func<ProcessSessionErrorEventArgs, Task>? _errorHandler;

    private IConsumer<string, byte[]>? _consumer;
    private CancellationTokenSource? _cts;
    private Task? _consumeLoop;

    /// <summary>
    /// Creates a processor, optionally with a <c>configureConsumer</c> hook applied to the
    /// <see cref="ConsumerConfig"/> after the processor's own settings — for anything that cannot
    /// live in configuration, such as SASL, SSL, timeouts and fetch tuning. The offset settings the
    /// ordering guarantee depends on are re-applied afterwards and cannot be overridden.
    /// </summary>
    public KafkaSessionProcessor(
        KafkaSessionProcessorOptions options,
        ILoggerFactory loggerFactory,
        Action<ConsumerConfig>? configureConsumer = null)
    {
        _options = options;
        _configureConsumer = configureConsumer;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KafkaSessionProcessor>();
        _partitionSlots = new SemaphoreSlim(options.MaxConcurrentPartitions);
        _sessionSlots = new SemaphoreSlim(options.MaxConcurrentSessions);
    }

    public KafkaSessionProcessor(
        IOptions<KafkaSessionProcessorOptions> options,
        ILoggerFactory loggerFactory)
        : this(options.Value, loggerFactory)
    {
    }

    /// <summary>
    /// Handles one session's messages. Exactly one handler may be registered, and it must be
    /// registered before processing starts.
    /// </summary>
    public event Func<ProcessSessionMessagesEventArgs, Task> ProcessSessionMessagesAsync
    {
        add => _messageHandler = SetHandler(_messageHandler, value, nameof(ProcessSessionMessagesAsync));
        remove => _messageHandler = ClearHandler(_messageHandler, value, nameof(ProcessSessionMessagesAsync));
    }

    /// <summary>
    /// Observes handler failures. Informational: the processor has already decided to block the
    /// partition and retry.
    /// </summary>
    public event Func<ProcessSessionErrorEventArgs, Task> ProcessErrorAsync
    {
        add => _errorHandler = SetHandler(_errorHandler, value, nameof(ProcessErrorAsync));
        remove => _errorHandler = ClearHandler(_errorHandler, value, nameof(ProcessErrorAsync));
    }

    public bool IsProcessing { get; private set; }

    public Task StartProcessingAsync(CancellationToken cancellationToken = default)
    {
        if (IsProcessing)
            throw new InvalidOperationException("The processor is already processing.");

        if (_messageHandler is null)
        {
            throw new InvalidOperationException(
                $"Subscribe to {nameof(ProcessSessionMessagesAsync)} before starting the processor.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _consumer = BuildConsumer();
        _consumer.Subscribe(_options.Topic);
        IsProcessing = true;

        // Long-running because librdkafka's Consume blocks; it must not sit on a pool thread.
        _consumeLoop = Task.Factory.StartNew(
            () => RunConsumeLoop(_cts.Token),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        _logger.LogInformation(
            "Kafka session processor started on topic {Topic} in group {Group}: "
            + "up to {Partitions} partition(s) and {Sessions} session(s) at once, {PerSession} message(s) per call",
            _options.Topic, _options.ConsumerGroup, _options.MaxConcurrentPartitions,
            _options.MaxConcurrentSessions, _options.MaxMessagesPerSession);

        return Task.CompletedTask;
    }

    public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
    {
        if (!IsProcessing)
            return;

        if (_cts is not null)
            await _cts.CancelAsync();

        if (_consumeLoop is not null)
            await _consumeLoop;

        // Close triggers a final revoke, which drains the workers and commits what they finished.
        _consumer?.Close();
        IsProcessing = false;

        _logger.LogInformation("Kafka session processor stopped");
    }

    public async ValueTask DisposeAsync()
    {
        await StopProcessingAsync();
        await StopRemainingWorkersAsync();

        _consumer?.Dispose();
        _cts?.Dispose();
        _partitionSlots.Dispose();
        _sessionSlots.Dispose();
    }

    private static Func<T, Task> SetHandler<T>(Func<T, Task>? current, Func<T, Task> value, string name)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (current is not null)
            throw new NotSupportedException($"Only one {name} handler may be registered.");

        return value;
    }

    private static Func<T, Task>? ClearHandler<T>(Func<T, Task>? current, Func<T, Task> value, string name)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (current != value)
            throw new NotSupportedException($"The {name} handler being removed is not the one registered.");

        return null;
    }

    private IConsumer<string, byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AutoCommitIntervalMs = _options.OffsetCommitIntervalMilliseconds,

            // Incremental rebalancing: adding or losing an instance only moves the partitions that
            // actually change hands, instead of stopping every worker in the group.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };

        _configureConsumer?.Invoke(config);

        // Re-applied after the caller's hook: the whole design rests on librdkafka committing only
        // offsets we have explicitly stored, and we only store one once every message below it has
        // been handed to a handler that returned.
        config.EnableAutoCommit = true;
        config.EnableAutoOffsetStore = false;

        return new ConsumerBuilder<string, byte[]>(config)
            .SetPartitionsAssignedHandler((_, partitions) => OnPartitionsAssigned(partitions))
            .SetPartitionsRevokedHandler((_, partitions) => OnPartitionsRevoked(partitions))
            .SetPartitionsLostHandler((_, partitions) => OnPartitionsLost(partitions))
            .SetErrorHandler((_, error) => _logger.LogError(
                "Kafka error {Code}: {Reason} (fatal: {IsFatal})", error.Code, error.Reason, error.IsFatal))
            .Build();
    }

    private void RunConsumeLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                StoreReadyPositions();
                ResumeDrainedPartitions();

                var result = _consumer!.Consume(PollTimeout);
                if (result is null || result.IsPartitionEOF)
                    continue;

                Dispatch(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume failed: {Reason}", ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in Kafka consume loop");
            }
        }

        StoreReadyPositions();
    }

    private void Dispatch(ConsumeResult<string, byte[]> result)
    {
        // Mid-rebalance we can still be handed a message for a partition we no longer own. Dropping
        // it is correct: it was never committed, so its new owner will read it.
        if (!_workers.TryGetValue(result.TopicPartition, out var worker))
            return;

        if (worker.TryEnqueue(result))
            return;

        // This partition's buffer is full. Pause *only* this partition so a slow session applies
        // backpressure to the broker rather than starving every other partition on the thread.
        _consumer!.Pause([result.TopicPartition]);
        _pausedWithStash[result.TopicPartition] = result;

        _logger.LogDebug("Paused {Partition}: buffer full", result.TopicPartition);
    }

    private void ResumeDrainedPartitions()
    {
        if (_pausedWithStash.Count == 0)
            return;

        foreach (var (partition, stashed) in _pausedWithStash.ToList())
        {
            if (!_workers.TryGetValue(partition, out var worker))
            {
                _pausedWithStash.Remove(partition);
                continue;
            }

            // The stashed message is the one Consume already handed us; it has to reach the buffer
            // before the partition may flow again, or it would be skipped.
            if (!worker.TryEnqueue(stashed))
                continue;

            _pausedWithStash.Remove(partition);
            _consumer!.Resume([partition]);

            _logger.LogDebug("Resumed {Partition}: buffer drained", partition);
        }
    }

    private void StoreReadyPositions()
    {
        while (_readyPositions.TryDequeue(out var position))
        {
            try
            {
                _consumer!.StoreOffset(position);
            }
            catch (KafkaException ex)
            {
                // Almost always "partition no longer assigned" — the new owner will re-read from
                // the last committed position, which is safe under an idempotent handler.
                _logger.LogDebug(ex, "Could not store offset for {Partition}", position.TopicPartition);
            }
        }
    }

    private void OnPartitionsAssigned(List<TopicPartition> partitions)
    {
        foreach (var partition in partitions)
        {
            var worker = new PartitionSessionWorker(
                partition,
                _options,
                _partitionSlots,
                _sessionSlots,
                _messageHandler!,
                _errorHandler,
                _readyPositions.Enqueue,
                _loggerFactory.CreateLogger<PartitionSessionWorker>());

            worker.Start(_cts!.Token);
            _workers[partition] = worker;
        }

        _logger.LogInformation(
            "Assigned {Count} partition(s): {Partitions}; now owning {Total}",
            partitions.Count, string.Join(", ", partitions), _workers.Count);
    }

    private void OnPartitionsRevoked(List<TopicPartitionOffset> partitions)
    {
        _logger.LogInformation("Revoking {Count} partition(s): {Partitions}",
            partitions.Count, string.Join(", ", partitions.Select(p => p.TopicPartition)));

        DrainWorkers(partitions.Select(p => p.TopicPartition), commit: true);
    }

    private void OnPartitionsLost(List<TopicPartitionOffset> partitions)
    {
        // The assignment is already gone, so committing would either fail or clobber the new
        // owner's progress. Drop everything in flight and let it be re-read.
        _logger.LogWarning("Lost {Count} partition(s): {Partitions}",
            partitions.Count, string.Join(", ", partitions.Select(p => p.TopicPartition)));

        DrainWorkers(partitions.Select(p => p.TopicPartition), commit: false);
    }

    // Runs on the consume thread inside a rebalance callback, so it blocks — but only for a bounded
    // time: overrunning max.poll.interval.ms would get this instance evicted from the group.
    private void DrainWorkers(IEnumerable<TopicPartition> partitions, bool commit)
    {
        var affected = partitions.ToHashSet();
        var stopping = new List<Task>();

        foreach (var partition in affected)
        {
            _pausedWithStash.Remove(partition);

            if (_workers.Remove(partition, out var worker))
                stopping.Add(worker.StopAsync());
        }

        if (stopping.Count > 0 && !Task.WhenAll(stopping).Wait(RebalanceDrainTimeout))
        {
            _logger.LogWarning(
                "Partition workers did not drain within {Timeout}s; their uncommitted messages will be re-read",
                RebalanceDrainTimeout.TotalSeconds);
        }

        if (!commit)
        {
            DiscardPositionsFor(affected);
            return;
        }

        StoreReadyPositions();
        CommitStoredPositions();
    }

    // Only the lost partitions' positions are dropped — positions belonging to partitions we still
    // own must survive, or their messages would be needlessly re-read.
    private void DiscardPositionsFor(HashSet<TopicPartition> lost)
    {
        var retained = new List<TopicPartitionOffset>();

        while (_readyPositions.TryDequeue(out var position))
        {
            if (!lost.Contains(position.TopicPartition))
                retained.Add(position);
        }

        foreach (var position in retained)
            _readyPositions.Enqueue(position);
    }

    private void CommitStoredPositions()
    {
        try
        {
            _consumer!.Commit();
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_NoOffset)
        {
            // Nothing was handled since the last commit.
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning(ex, "Commit on revoke failed; the affected messages will be re-read");
        }
    }

    private async Task StopRemainingWorkersAsync()
    {
        foreach (var worker in _workers.Values)
            await worker.StopAsync();

        _workers.Clear();
        _pausedWithStash.Clear();
    }
}
