using Azure.Messaging.ServiceBus;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public static class ServiceBusRegisterExtension
{
    public static void AddServiceBusListener(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddServiceBusListenerOptions(configuration);
        services.AddServiceBusClient(configuration);
        services.AddEventProcessingServices();
    }

    private static void AddServiceBusListenerOptions(this IServiceCollection services, IConfiguration configuration)
    {
        // Validation runs when the options are first resolved — i.e. when the
        // ServiceBusEventListener is constructed at host startup. ValidateOnStart is
        // intentionally omitted so test hosts that remove the listener don't trip on the
        // empty appsettings placeholders (real values come from secrets/env at runtime).
        services.AddOptions<ServiceBusListenerOptions>()
            .Bind(configuration.GetSection(ServiceBusListenerOptions.SectionName))
            .ValidateDataAnnotations();
    }

    private static void AddServiceBusClient(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(_ =>
            new ServiceBusClient(configuration.GetConnectionString("ServiceBus")));
    }

    private static void AddEventProcessingServices(this IServiceCollection services)
    {
        services.AddSingleton<IEventDeserializer, EventDeserializer>();
        services.AddSingleton<EventBatchProcessor>();
        services.AddHostedService<ServiceBusEventListener>();
    }
}
