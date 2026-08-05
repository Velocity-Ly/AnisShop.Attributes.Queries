using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.Messaging;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;
using AnisShop.Kafka.Sessions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AnisShop.Attributes.Queries.Tests.Kafka
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
        public void AddEventListener_ServiceBus_RegistersOnlyTheServiceBusListener()
        {
            // Arrange
            var services = BuildServices("ServiceBus");

            // Act
            var hostedServices = HostedServiceTypes(services);

            // Assert
            Assert.Contains(typeof(ServiceBusEventListener), hostedServices);
            Assert.DoesNotContain(typeof(KafkaEventListener), hostedServices);
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
    }
}
