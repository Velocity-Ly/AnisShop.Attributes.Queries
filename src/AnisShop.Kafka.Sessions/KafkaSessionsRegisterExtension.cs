using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AnisShop.Kafka.Sessions;

public static class KafkaSessionsRegisterExtension
{
    /// <summary>
    /// Binds <see cref="KafkaSessionProcessorOptions"/> and registers a
    /// <see cref="KafkaSessionProcessor"/> singleton. Subscribing to it and starting it is the
    /// host's job — write a small <c>IHostedService</c>, exactly as you would for a
    /// <c>ServiceBusSessionProcessor</c>.
    /// </summary>
    /// <remarks>
    /// Options are validated when they are first resolved (processor construction) rather than via
    /// <c>ValidateOnStart</c>, so a test host that never starts the processor can still boot against
    /// empty configuration placeholders.
    /// <para>
    /// For more than one topic in a process, construct additional processors directly with the
    /// <see cref="KafkaSessionProcessor(KafkaSessionProcessorOptions, Microsoft.Extensions.Logging.ILoggerFactory, System.Action{Confluent.Kafka.ConsumerConfig})"/>
    /// constructor — this registration binds a single unnamed options instance.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddKafkaSessionProcessor(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaSessionProcessorOptions.SectionName)
    {
        services.AddOptions<KafkaSessionProcessorOptions>()
            .Bind(configuration.GetSection(sectionName))
            .ValidateDataAnnotations();

        services.TryAddSingleton<KafkaSessionProcessor>();

        return services;
    }
}
