using AnisShop.Kafka.Sessions;

namespace AnisShop.Attributes.Queries.Infrastructure.Kafka;

public static class KafkaRegisterExtension
{
    // The processor comes from AnisShop.Kafka.Sessions and gives us session-shaped delivery.
    // KafkaEventListener is ours, and is the same shape as ServiceBusEventListener: subscribe,
    // start on host start, stop on host stop.
    public static void AddKafkaListener(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddKafkaSessionProcessor(configuration);
        services.AddSingleton<IKafkaEventDeserializer, KafkaEventDeserializer>();
        services.AddHostedService<KafkaEventListener>();
    }
}
