using Mediator;

namespace AnisShop.Attributes.Queries.Features.EventsHandler
{
    public class IncomingEventsHandler : IRequestHandler<IncomingEvents, bool>
    {
        public async ValueTask<bool> Handle(IncomingEvents request, CancellationToken cancellationToken)
        {
            return true;
        }
    }
}
