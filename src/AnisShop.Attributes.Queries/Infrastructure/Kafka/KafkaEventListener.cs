using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Features.EventsHandler;
using AnisShop.Kafka.Sessions;
using Mediator;

namespace AnisShop.Attributes.Queries.Infrastructure.Kafka;

public class KafkaEventListener : IHostedService
{
    private readonly KafkaSessionProcessor _processor;
    private readonly IKafkaEventDeserializer _deserializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaEventListener> _logger;

    public KafkaEventListener(
        KafkaSessionProcessor processor,
        IKafkaEventDeserializer deserializer,
        IServiceScopeFactory scopeFactory,
        ILogger<KafkaEventListener> logger)
    {
        _processor = processor;
        _deserializer = deserializer;
        _scopeFactory = scopeFactory;
        _logger = logger;

        _processor.ProcessSessionMessagesAsync += ProcessSessionAsync;
        _processor.ProcessErrorAsync += OnErrorAsync;
    }

    public Task StartAsync(CancellationToken cancellationToken) =>
        _processor.StartProcessingAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        _processor.StopProcessingAsync(cancellationToken);

    public async Task ProcessSessionAsync(ProcessSessionMessagesEventArgs args)
    {
        var events = new List<EventBase>(args.Messages.Count);

        foreach (var message in args.Messages)
        {
            var @event = _deserializer.Deserialize(message);
            if (@event is null)
            {
                throw new InvalidOperationException(
                    $"Cannot deserialize the message at {args.Partition} offset {message.Offset.Value} "
                    + $"for aggregate {args.SessionId}.");
            }

            events.Add(@event);
        }

        using var scope = _scopeFactory.CreateScope();
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        // IncomingEventsHandler is idempotent per (aggregate, version) — it has to be, because the
        // partition cursor replays a tail after any restart or rebalance. It returns false only
        // when the read model is not yet at Events[0].Version - 1.
        var applied = await mediator.Send(new IncomingEvents { Events = events }, args.CancellationToken);

        if (applied)
            return;

        // Under the ordering the publisher promises, this cannot happen: a session's events arrive
        // in publish order, so version N-1 was always handled before version N. Reaching here means
        // the promise was broken — events published out of order, or a read model rebuilt from an
        // incomplete log — and it must be loud, not silently skipped.
        throw new InvalidOperationException(
            $"Version gap for aggregate {args.SessionId} on {args.Partition}: the read model is not at "
            + $"version {events[0].Version - 1}. The publisher's ordering guarantee has been violated.");
    }

    private Task OnErrorAsync(ProcessSessionErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception, "Projection failed for aggregate {AggregateId} on {Partition}",
            args.SessionId, args.Partition);

        return Task.CompletedTask;
    }
}
