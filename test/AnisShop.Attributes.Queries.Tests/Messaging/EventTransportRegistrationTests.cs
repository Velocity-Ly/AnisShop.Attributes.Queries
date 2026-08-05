using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;
using AnisShop.Attributes.Queries.Infrastructure.Messaging;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;
using AnisShop.Kafka.Sessions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AnisShop.Attributes.Queries.Tests.Messaging
{
    // Exactly one transport must be live: two listeners on the same read model would race to
    // project the same streams.
    public class EventTransportRegistrationTests
    {
        [Fact]
        public void AddEventListener_Kafka_RegistersOnlyTheKafkaListener()
        {
            // Arrange
            var services = BuildServices("Kafka");

            // Act
            var hostedServices = HostedServiceTypes(services);

            // Assert
            Assert.Contains(typeof(KafkaEventListener), hostedServices);
            Assert.DoesNotContain(typeof(ServiceBusEventListener), hostedServices);
            Assert.DoesNotContain(hostedServices, IsOwnedByKafkaFlow);
        }

        [Fact]
        public void AddEventListener_Kafka_RegistersTheSessionProcessorAndOurDeserializer()
        {
            // Arrange: the package supplies session-shaped delivery; this application supplies the
            // envelope reader and the listener that decides what the messages mean.
            var services = BuildServices("Kafka");

            // Act + Assert
            Assert.Contains(services, d => d.ServiceType == typeof(KafkaSessionProcessor));

            var deserializer = Assert.Single(services, d => d.ServiceType == typeof(IKafkaEventDeserializer));
            Assert.Equal(typeof(KafkaEventDeserializer), deserializer.ImplementationType);
        }

        [Fact]
        public void AddEventListener_KafkaFlow_RegistersOnlyTheKafkaFlowConsumer()
        {
            // Arrange
            var services = BuildServices("KafkaFlow");

            // Act
            var hostedServices = HostedServiceTypes(services);

            // Assert: the hosted service comes out of the package — there is none of ours to run
            Assert.Contains(hostedServices, IsOwnedByKafkaFlow);
            Assert.DoesNotContain(typeof(KafkaEventListener), hostedServices);
            Assert.DoesNotContain(typeof(ServiceBusEventListener), hostedServices);
        }

        [Fact]
        public void AddEventListener_KafkaFlow_RegistersOurMiddlewareAndItsProjector()
        {
            // Arrange: the entire application-owned surface of this transport
            var services = BuildServices("KafkaFlow");

            // Act + Assert
            Assert.Contains(services, d => d.ServiceType == typeof(EventProjectionMiddleware));
            Assert.Contains(services, d => d.ServiceType == typeof(KafkaFlowEventProjector));

            var deserializer = Assert.Single(services, d => d.ServiceType == typeof(IKafkaFlowEventDeserializer));
            Assert.Equal(typeof(KafkaFlowEventDeserializer), deserializer.ImplementationType);
        }

        [Fact]
        public void AddEventListener_ServiceBus_RegistersOnlyTheServiceBusListener()
        {
            // Arrange
            var services = BuildServices("ServiceBus");

            // Act
            var hostedServices = HostedServiceTypes(services);

            // Assert
            Assert.Contains(typeof(ServiceBusEventListener), hostedServices);
            Assert.DoesNotContain(typeof(KafkaEventListener), hostedServices);
            Assert.DoesNotContain(hostedServices, IsOwnedByKafkaFlow);
            Assert.Contains(services, d => d.ServiceType == typeof(IEventDeserializer));
            Assert.Contains(services, d => d.ServiceType == typeof(EventBatchProcessor));
        }

        [Fact]
        public void AddEventListener_NoTransportConfigured_DefaultsToServiceBus()
        {
            // Arrange: the setting is absent, as it is for every deployment that predates Kafka
            var services = BuildServices(transport: null);

            // Act
            var hostedServices = HostedServiceTypes(services);

            // Assert
            Assert.Contains(typeof(ServiceBusEventListener), hostedServices);
            Assert.DoesNotContain(typeof(KafkaEventListener), hostedServices);
        }

        private static IServiceCollection BuildServices(string? transport)
        {
            var settings = new Dictionary<string, string?>
            {
                ["ConnectionStrings:ServiceBus"] = string.Empty,

                // KafkaFlow builds its whole topology while AddKafka runs, so unlike the other two
                // it cannot be registered against the empty appsettings placeholders.
                ["KafkaFlow:BootstrapServers"] = "broker:9092",
                ["KafkaFlow:Topic"] = "attributes-events",
                ["KafkaFlow:ConsumerGroup"] = "tests",
            };

            if (transport is not null)
                settings[EventTransportRegisterExtension.TransportKey] = transport;

            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(settings)
                .Build();

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddEventListener(configuration);

            return services;
        }

        private static IReadOnlyList<Type?> HostedServiceTypes(IServiceCollection services) =>
            [.. services.Where(d => d.ServiceType == typeof(IHostedService)).Select(d => d.ImplementationType)];

        // KafkaFlow's hosted service is internal to the package, so where it came from is the only
        // thing that identifies it.
        private static bool IsOwnedByKafkaFlow(Type? implementationType) =>
            implementationType?.Assembly.GetName().Name?.StartsWith("KafkaFlow", StringComparison.Ordinal) is true;
    }
}
