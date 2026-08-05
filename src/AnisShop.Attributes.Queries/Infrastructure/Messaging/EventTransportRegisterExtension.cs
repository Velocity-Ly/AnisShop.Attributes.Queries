using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

namespace AnisShop.Attributes.Queries.Infrastructure.Messaging;

public enum EventTransport
{
    ServiceBus,
    Kafka,
}

public static class EventTransportRegisterExtension
{
    public const string TransportKey = "Messaging:Transport";

    // Both listeners end at the same IncomingEvents projection, so the transport is a deployment
    // choice rather than an architectural one. Exactly one is registered — running both would have
    // two consumers racing to project the same streams.
    public static void AddEventListener(this IServiceCollection services, IConfiguration configuration)
    {
        var transport = configuration.GetValue(TransportKey, EventTransport.ServiceBus);

        switch (transport)
        {
            case EventTransport.Kafka:
                services.AddKafkaListener(configuration);
                break;

            case EventTransport.ServiceBus:
                services.AddServiceBusListener(configuration);
                break;

            default:
                throw new InvalidOperationException($"Unsupported event transport '{transport}'.");
        }
    }
}
