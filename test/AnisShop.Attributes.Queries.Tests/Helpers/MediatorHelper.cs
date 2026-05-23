using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Features.EventsHandler;
using Mediator;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public class MediatorHelper(WebApplicationFactory<Program> factory)
    {
        public async Task<bool> SendEvents(IReadOnlyCollection<EventBase> events)
        {
            using var scope = factory.Services.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            return await mediator.Send(new IncomingEvents { Events = events });
        }

        public async Task<bool> SendEvents(params EventBase[] events)
        {
            return await SendEvents((IReadOnlyCollection<EventBase>)events);
        }
    }
}
