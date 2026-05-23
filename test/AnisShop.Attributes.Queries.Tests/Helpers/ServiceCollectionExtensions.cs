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
            RemoveServiceBusServices(services);
            UseInMemoryDb(services);
        }

        private static void RemoveServiceBusServices(IServiceCollection services)
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
