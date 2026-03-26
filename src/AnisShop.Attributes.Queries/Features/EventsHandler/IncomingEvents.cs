using AnisShop.Attributes.Queries.Events;
using Mediator;

namespace AnisShop.Attributes.Queries.Features.EventsHandler
{
    public class IncomingEvents : IRequest<bool>
    {
        public required IReadOnlyCollection<EventBase> Events { get; init; }
    }
}
