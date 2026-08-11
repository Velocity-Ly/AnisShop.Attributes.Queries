using KafkaFlow;
using KafkaFlow.Consumers.DistributionStrategies;

namespace AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;

public static class KafkaFlowRegisterExtension
{
    // The whole listener. Compare with AddKafkaListener, which registers a processor, an envelope
    // reader and a hosted service of our own: here the consumer, the worker pool, the buffering,
    // the offset manager and the hosted service all come out of the package, and the application
    // contributes one middleware.
    public static void AddKafkaFlowListener(this IServiceCollection services, IConfiguration configuration)
    {
        var options = KafkaFlowListenerOptions.Read(configuration);

        // The projector reads IgnoreUnrecognizedMessages off this same instance.
        services.AddSingleton(options);
        services.AddSingleton<IKafkaFlowEventDeserializer, KafkaFlowEventDeserializer>();
        services.AddSingleton<KafkaFlowEventProjector>();

        services.AddKafkaFlowHostedService(kafka => kafka
            .UseMicrosoftLog()
            .AddCluster(cluster => cluster
                .WithBrokers(options.BootstrapServers.Split(',', StringSplitOptions.TrimEntries))
                // Left untouched on Plaintext so an unsecured broker still needs no configuration at
                // all. librdkafka reads the whole security posture from these four values.
                .WithSecurityInformation(security =>
                {
                    security.SecurityProtocol = options.SecurityProtocol;

                    if (!options.UsesSasl)
                        return;

                    security.SaslMechanism = options.SaslMechanism;
                    security.SaslUsername = options.SaslUsername;
                    security.SaslPassword = options.SaslPassword;
                })
                .AddConsumer(consumer => consumer
                    .Topic(options.Topic)
                    .WithGroupId(options.ConsumerGroup)
                    .WithWorkersCount(options.WorkersCount)
                    .WithBufferSize(options.BufferSize)
                    // A projection that has never run has to start from the beginning of the log,
                    // not from whatever is being published while it boots.
                    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                    // Explicit although it is the default: hashing the key onto a fixed worker is
                    // the entire ordering guarantee, and it should not be able to change silently.
                    .WithWorkerDistributionStrategy<BytesSumDistributionStrategy>()
                    .AddMiddlewares(middlewares => middlewares
                        .AddBatching(
                            options.BatchSize,
                            TimeSpan.FromMilliseconds(options.BatchTimeoutMilliseconds))
                        .Add<EventProjectionMiddleware>()))));
    }
}
