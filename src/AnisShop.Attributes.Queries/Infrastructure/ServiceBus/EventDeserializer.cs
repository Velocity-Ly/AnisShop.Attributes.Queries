using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Messaging;
using Azure.Messaging.ServiceBus;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public class EventDeserializer : EventPayloadDeserializer, IEventDeserializer
{
    public EventDeserializer(ILogger<EventDeserializer> logger)
        : base(logger)
    {
    }

    public EventBase? Deserialize(ServiceBusReceivedMessage message)
    {
        var typeName = message.Subject
            ?? message.ApplicationProperties.GetValueOrDefault("Type") as string;

        return DeserializePayload(typeName, message.Body.ToMemory());
    }
}
