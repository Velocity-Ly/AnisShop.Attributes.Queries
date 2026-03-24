using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.Tests.FakeServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public static class ServiceCollectionExtensions
    {
        public static void SetDefaultUnitTestsEnvironment(this IServiceCollection services)
        {
            RemoveDatabaseRunner(services);
            UseInMemoryDb(services);
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
