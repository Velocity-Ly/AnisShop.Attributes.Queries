using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public class DatabaseHelper(WebApplicationFactory<Program> factory)
    {
        public async ValueTask<T> Query<T>(Func<AttributesDbContext, ValueTask<T>> query)
        {
            using var scope = factory.Services.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            return await query(dbContext);
        }

        public async Task<T> Query<T>(Func<AttributesDbContext, Task<T>> query)
        {
            using var scope = factory.Services.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            return await query(dbContext);
        }

        public void Seed(Action<AttributesDbContext> seed)
        {
            using var scope = factory.Services.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            seed(dbContext);
            dbContext.SaveChanges();
        }

        public async Task<SourceDomain.Attribute> InsertAsync(SourceDomain.Attribute attribute)
        {
            using var scope = factory.Services.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            await dbContext.Attributes.AddAsync(attribute);
            await dbContext.SaveChangesAsync();
            return attribute;
        }

        public async Task<SourceDomain.Attribute[]> InsertAsync(SourceDomain.Attribute[] attributes)
        {
            using var scope = factory.Services.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            await dbContext.Attributes.AddRangeAsync(attributes);
            await dbContext.SaveChangesAsync();
            return attributes;
        }

        public async Task<SourceDomain.Attribute> InsertAsync(AttributeFaker attributeFaker)
        {
            var attribute = attributeFaker.Generate();
            using var scope = factory.Services.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            await dbContext.Attributes.AddAsync(attribute);
            await dbContext.SaveChangesAsync();
            return attribute;
        }
    }
}
