using Confluent.Kafka;

namespace AnisShop.Kafka.Sessions;

/// <summary>
/// One session's messages, in the order the sender produced them.
/// </summary>
/// <remarks>
/// The counterpart of <c>ProcessSessionMessageEventArgs</c>, except that it carries a run of
/// messages rather than one. Everything about a message stays raw — deserialize
/// <see cref="ConsumeResult{TKey,TValue}.Message"/> yourself, exactly as you would a
/// <c>ServiceBusReceivedMessage</c>.
/// <para>
/// Returning from the handler means "done": the offsets may move past these messages. Throwing
/// means "not done": the partition blocks and the same messages come back on the next attempt.
/// </para>
/// </remarks>
public sealed class ProcessSessionMessagesEventArgs
{
    public ProcessSessionMessagesEventArgs(
        string sessionId,
        IReadOnlyList<ConsumeResult<string, byte[]>> messages,
        TopicPartition partition,
        CancellationToken cancellationToken)
    {
        SessionId = sessionId;
        Messages = messages;
        Partition = partition;
        CancellationToken = cancellationToken;
    }

    /// <summary>
    /// The message key the sender set. Empty for messages produced without a key — see
    /// <see cref="KafkaSessionProcessor"/> for what that means.
    /// </summary>
    public string SessionId { get; }

    /// <summary>
    /// Up to <c>MaxMessagesPerSession</c> messages, all from this session, in production order.
    /// Never empty.
    /// </summary>
    public IReadOnlyList<ConsumeResult<string, byte[]>> Messages { get; }

    /// <summary>The partition these messages came off. Useful for logging and metrics.</summary>
    public TopicPartition Partition { get; }

    /// <summary>Cancelled when the processor stops or this partition is revoked.</summary>
    public CancellationToken CancellationToken { get; }
}

/// <summary>
/// Raised when a handler throws, so failures are observable rather than only logged.
/// </summary>
/// <remarks>
/// Informational only — the processor has already decided what to do (block the partition and
/// retry). An exception thrown from this handler is swallowed.
/// </remarks>
public sealed class ProcessSessionErrorEventArgs
{
    public ProcessSessionErrorEventArgs(
        Exception exception,
        string sessionId,
        TopicPartition partition,
        CancellationToken cancellationToken)
    {
        Exception = exception;
        SessionId = sessionId;
        Partition = partition;
        CancellationToken = cancellationToken;
    }

    public Exception Exception { get; }

    public string SessionId { get; }

    public TopicPartition Partition { get; }

    public CancellationToken CancellationToken { get; }
}
