using System.ComponentModel.DataAnnotations;

namespace AnisShop.Kafka.Sessions;

public class KafkaSessionProcessorOptions : IValidatableObject
{
    public const string SectionName = "Kafka";

    [Required]
    public required string BootstrapServers { get; init; }

    [Required]
    public required string Topic { get; init; }

    // Kafka's unit of horizontal scale-out. Every instance sharing this group id is assigned a
    // disjoint slice of the partitions, so adding pods multiplies throughput up to the partition
    // count — the equivalent of running more session receivers.
    [Required]
    public required string ConsumerGroup { get; init; }

    // How many partitions may be handling a batch at the same moment. Service Bus has no equivalent
    // because it has no partitions to own; here it is the ceiling on in-flight work per process and
    // therefore on memory.
    [Range(1, int.MaxValue)]
    public int MaxConcurrentPartitions { get; init; } = 32;

    // The direct counterpart of ServiceBusSessionProcessor.MaxConcurrentSessions: how many sessions
    // are handled at once across every owned partition.
    [Range(1, int.MaxValue)]
    public int MaxConcurrentSessions { get; init; } = 1000;

    // The counterpart of ServiceBusSessionProcessor.MaxMessagesPerSession, and the one real
    // difference in the handler contract: the handler is called with up to this many messages of
    // one session at a time, instead of one message per call. A session holding more than this is
    // delivered in several calls, back to back, still in arrival order.
    [Range(1, int.MaxValue)]
    public int MaxMessagesPerSession { get; init; } = 100;

    // Bounded per-partition buffer, and the ceiling on one batch. When it fills, that partition —
    // and only that partition — is paused on the broker, so a slow session never starves its
    // neighbours.
    [Range(1, int.MaxValue)]
    public int PartitionBufferSize { get; init; } = 4000;

    // Wait this long for stragglers before handling a partial batch. Trades a little latency for
    // fatter batches, which surface more distinct sessions per pass and widen the fan-out.
    [Range(0, 60_000)]
    public int BatchLingerMilliseconds { get; init; } = 25;

    // Longest one handler call may run before the attempt is abandoned and retried. Guards against
    // a wedged handler holding a batch open with no progress and no diagnostics.
    [Range(1_000, 600_000)]
    public int HandlerTimeoutMilliseconds { get; init; } = 60_000;

    // Nothing is ever discarded, so a partition that cannot make progress simply retries. The delay
    // doubles per consecutive blocked cycle up to the cap, which keeps a long outage from being
    // hammered while still recovering promptly from a brief one.
    [Range(1, 60_000)]
    public int RetryBackoffMilliseconds { get; init; } = 200;

    [Range(1, 600_000)]
    public int MaxRetryBackoffMilliseconds { get; init; } = 30_000;

    // How often librdkafka flushes the offsets we have stored. Offsets are only ever *stored* after
    // a handler has returned, so a commit can never run ahead of the consumer.
    [Range(100, 60_000)]
    public int OffsetCommitIntervalMilliseconds { get; init; } = 5_000;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        // A partition that grabs a slot and then cannot get a single session slot holds the slot
        // while doing nothing, so the effective partition concurrency silently drops.
        if (MaxConcurrentSessions < MaxConcurrentPartitions)
        {
            yield return new ValidationResult(
                $"{nameof(MaxConcurrentSessions)} ({MaxConcurrentSessions}) must be at least "
                + $"{nameof(MaxConcurrentPartitions)} ({MaxConcurrentPartitions}); otherwise a partition "
                + "can hold a slot without being able to handle any session.",
                [nameof(MaxConcurrentSessions), nameof(MaxConcurrentPartitions)]);
        }
    }
}
