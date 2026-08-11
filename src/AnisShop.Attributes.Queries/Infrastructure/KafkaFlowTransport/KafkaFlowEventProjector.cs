using System.Text;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Features.EventsHandler;
using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using KafkaFlow;
using Mediator;

namespace AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;

// The entire application-owned part of the KafkaFlow listener.
//
// A worker hands over everything it collected since its last dispatch. KafkaFlow guarantees only
// that one message key never moves between workers, so that batch is a hash bucket and not an
// aggregate: it holds every key that happened to hash to this worker, and can span partitions.
// Regrouping by key turns it back into runs the projection can accept.
public class KafkaFlowEventProjector
{
    private readonly IKafkaFlowEventDeserializer _deserializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly bool _ignoreUnrecognizedMessages;
    private readonly ILogger<KafkaFlowEventProjector> _logger;

    public KafkaFlowEventProjector(
        IKafkaFlowEventDeserializer deserializer,
        IServiceScopeFactory scopeFactory,
        KafkaFlowListenerOptions options,
        ILogger<KafkaFlowEventProjector> logger)
    {
        _deserializer = deserializer;
        _scopeFactory = scopeFactory;
        _ignoreUnrecognizedMessages = options.IgnoreUnrecognizedMessages;
        _logger = logger;
    }

    public async Task ProjectAsync(IReadOnlyCollection<IMessageContext> batch, CancellationToken cancellationToken)
    {
        // GroupBy keeps each group in arrival order, and for one key arrival order is publish
        // order. Aggregates run one after another here: the worker is KafkaFlow's unit of
        // concurrency, and fanning out inside it would put two calls on the same connection pool
        // slot for no ordering benefit.
        foreach (var aggregate in batch.GroupBy(AggregateIdOf))
            await ProjectAggregateAsync(aggregate.Key, [.. aggregate], cancellationToken);
    }

    private async Task ProjectAggregateAsync(
        string aggregateId,
        IReadOnlyList<IMessageContext> messages,
        CancellationToken cancellationToken)
    {
        var events = new List<EventBase>(messages.Count);

        foreach (var message in messages)
        {
            var @event = _deserializer.Deserialize(message);
            if (@event is null)
            {
                // A message with no recognised type header was never one of ours. On a shared topic
                // that is foreign traffic (a health probe, a smoke test) and skipping it is correct;
                // failing would drop this worker's whole batch for a message we don't even own. A
                // message that DOES carry a type header but still won't deserialize is our data going
                // wrong, so it stays fatal regardless of this switch.
                if (_ignoreUnrecognizedMessages && !HasTypeHeader(message))
                {
                    _logger.LogDebug(
                        "Skipping foreign message on {Topic}/{Partition} at offset {Offset}: no event type header.",
                        message.ConsumerContext.Topic,
                        message.ConsumerContext.Partition,
                        message.ConsumerContext.Offset);
                    continue;
                }

                throw Fail(
                    aggregateId, messages,
                    $"the message at offset {message.ConsumerContext.Offset} cannot be deserialized");
            }

            events.Add(@event);
        }

        // Everything in this group was foreign and skipped — there is nothing to project.
        if (events.Count == 0)
            return;

        using var scope = _scopeFactory.CreateScope();
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        // IncomingEventsHandler is idempotent per (aggregate, version) — it has to be, because a
        // rebalance replays whatever was not committed. It returns false only when the read model
        // is not yet at Events[0].Version - 1.
        if (await mediator.Send(new IncomingEvents { Events = events }, cancellationToken))
            return;

        // Under the ordering the publisher promises this cannot happen: one aggregate's events are
        // produced under one key, so version N-1 was handled before version N.
        throw Fail(aggregateId, messages, $"the read model is not at version {events[0].Version - 1}");
    }

    // KafkaFlow's batching middleware catches whatever comes out of here, writes one line through
    // the log handler and then completes every message in the batch — so throwing skips this
    // aggregate *and* every other one this worker happened to collect. Naming them here is what
    // makes that loss traceable rather than a count in a generic error line.
    private InvalidOperationException Fail(
        string aggregateId,
        IReadOnlyList<IMessageContext> messages,
        string reason)
    {
        _logger.LogCritical(
            "Skipping {Count} events for aggregate {AggregateId} on {Topic}/{Partition} at offsets {Offsets}: {Reason}",
            messages.Count,
            aggregateId,
            messages[0].ConsumerContext.Topic,
            messages[0].ConsumerContext.Partition,
            string.Join(", ", messages.Select(message => message.ConsumerContext.Offset)),
            reason);

        return new InvalidOperationException(
            $"Cannot project events for aggregate {aggregateId}: {reason}.");
    }

    // The message key is the aggregate id, set by the publisher — the same field the hand-rolled
    // listener treats as a session id. A message with no key lands on worker 0 alongside every
    // other keyless message, so grouping them together keeps them one ordered run.
    private static string AggregateIdOf(IMessageContext message) =>
        message.Message.Key is byte[] key ? Encoding.UTF8.GetString(key) : string.Empty;

    // The publisher stamps every event with a type header (either spelling). Its absence is what
    // distinguishes another producer's message from one of ours that merely failed to parse.
    private static bool HasTypeHeader(IMessageContext message) =>
        message.Headers[KafkaEventDeserializer.TypeHeader] is not null
        || message.Headers[KafkaEventDeserializer.LegacyTypeHeader] is not null;
}
