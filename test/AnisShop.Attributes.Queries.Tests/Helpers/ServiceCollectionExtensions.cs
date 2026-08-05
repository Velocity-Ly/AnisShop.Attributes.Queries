using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;
using AnisShop.Attributes.Queries.Tests.FakeServices;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public static class ServiceCollectionExtensions
    {
        public static void SetDefaultUnitTestsEnvironment(this IServiceCollection services)
        {
            RemoveDatabaseRunner(services);
            RemoveEventListeners(services);
            UseInMemoryDb(services);
        }

        // Only one transport is ever registered (see Messaging:Transport), but the test host must
        // boot whichever one appsettings happens to select, so all three are stripped
        // unconditionally.
        public static void RemoveEventListeners(this IServiceCollection services)
        {
            RemoveServiceBusServices(services);
            RemoveKafkaServices(services);
            RemoveKafkaFlowServices(services);
        }

        // Strips the live Service Bus listener + client so the test host can boot without a
        // real Azure connection string. Shared by both the unit (InMemory) and integration
        // (LocalDB) environments — both project events directly via Mediator, never over the wire.
        public static void RemoveServiceBusServices(this IServiceCollection services)
        {
            var serviceBusClient = services.SingleOrDefault(d => d.ServiceType == typeof(ServiceBusClient));
            if (serviceBusClient != null)
                services.Remove(serviceBusClient);

            var hostedServices = services
                .Where(d => d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(ServiceBusEventListener))
                .ToList();

            foreach (var descriptor in hostedServices)
                services.Remove(descriptor);
        }

        // Same idea for Kafka: the listener starts the session processor in StartAsync, which
        // resolves options that would trip on the empty appsettings placeholders. The deserializer
        // and the processor registration stay, so a test can still drive the handler directly.
        public static void RemoveKafkaServices(this IServiceCollection services)
        {
            var hostedServices = services
                .Where(d => d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(KafkaEventListener))
                .ToList();

            foreach (var descriptor in hostedServices)
                services.Remove(descriptor);
        }

        // And for KafkaFlow, whose hosted service starts a real consumer against whatever brokers
        // it was configured with. It is internal to the package, so it is identified by origin —
        // KafkaFlow contributes no other hosted service.
        public static void RemoveKafkaFlowServices(this IServiceCollection services)
        {
            var hostedServices = services
                .Where(d => d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType?.Assembly.GetName().Name?
                        .StartsWith("KafkaFlow", StringComparison.Ordinal) is true)
                .ToList();

            foreach (var descriptor in hostedServices)
                services.Remove(descriptor);
        }

        private static void RemoveDatabaseRunner(IServiceCollection services)
        {
            var descriptor = services.SingleOrDefault(d => d.ImplementationType == typeof(DatabaseRunner));
            if (descriptor != null)
            {
                services.Remove(descriptor);
            }
        }

        private static void UseInMemoryDb(IServiceCollection services)
        {
            var optionsConfig = services
                .Where(r => r.ServiceType.IsGenericType && r.ServiceType.GetGenericTypeDefinition() == typeof(IDbContextOptionsConfiguration<>))
                .SingleOrDefault();

            if (optionsConfig != null)
            {
                services.Remove(optionsConfig);
            }

            var dbContextDescriptor = services.SingleOrDefault(d => d.ServiceType == typeof(AttributesDbContext));
            if (dbContextDescriptor != null)
            {
                services.Remove(dbContextDescriptor);
            }

            var dbName = Guid.NewGuid().ToString();
            services.AddScoped<AttributesDbContext>(sp =>
            {
                var optionsBuilder = new DbContextOptionsBuilder<AttributesDbContext>();
                optionsBuilder.UseInMemoryDatabase(dbName);
                return new FakeAttributesDbContext(optionsBuilder.Options);
            });
        }
    }
}
