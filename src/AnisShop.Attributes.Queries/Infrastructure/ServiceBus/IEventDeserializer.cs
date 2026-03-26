using AnisShop.Attributes.Queries.Events;
using Azure.Messaging.ServiceBus;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public interface IEventDeserializer
{
    EventBase? Deserialize(ServiceBusReceivedMessage message);
}
