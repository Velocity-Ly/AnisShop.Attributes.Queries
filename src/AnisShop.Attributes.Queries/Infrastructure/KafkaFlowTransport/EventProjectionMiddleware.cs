using KafkaFlow;

namespace AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;

// Everything this listener adds to KafkaFlow's pipeline. AddBatching sits in front of it and turns
// a stream of single messages into one call per worker dispatch; there is no hosted service, no
// consume loop and no offset bookkeeping to write, because the package owns all three.
public class EventProjectionMiddleware : IMessageMiddleware
{
    private readonly KafkaFlowEventProjector _projector;

    public EventProjectionMiddleware(KafkaFlowEventProjector projector)
    {
        _projector = projector;
    }

    public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
    {
        await _projector.ProjectAsync(context.GetMessagesBatch(), context.ConsumerContext.WorkerStopped);

        await next(context);
    }
}
