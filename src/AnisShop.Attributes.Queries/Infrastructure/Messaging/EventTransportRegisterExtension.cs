using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

namespace AnisShop.Attributes.Queries.Infrastructure.Messaging;

public enum EventTransport
{
    ServiceBus,
    Kafka,
    KafkaFlow,
}

public static class EventTransportRegisterExtension
{
    public const string TransportKey = "Messaging:Transport";

    // All three listeners end at the same IncomingEvents projection, so the transport is a
    // deployment choice rather than an architectural one. Exactly one is registered — running two
    // would have both racing to project the same streams.
    //
    // Kafka and KafkaFlow both read the same topic in the same envelope; they differ in who owns
    // the consumer. Kafka drives AnisShop.Kafka.Sessions, which this repository maintains;
    // KafkaFlow hands that job to the package. See docs/kafkaflow-listener.md.
    public static void AddEventListener(this IServiceCollection services, IConfiguration configuration)
    {
        var transport = configuration.GetValue(TransportKey, EventTransport.ServiceBus);

        switch (transport)
        {
            case EventTransport.Kafka:
                services.AddKafkaListener(configuration);
                break;

            case EventTransport.KafkaFlow:
                services.AddKafkaFlowListener(configuration);
                break;

            case EventTransport.ServiceBus:
                services.AddServiceBusListener(configuration);
                break;

            default:
                throw new InvalidOperationException($"Unsupported event transport '{transport}'.");
        }
    }
}
