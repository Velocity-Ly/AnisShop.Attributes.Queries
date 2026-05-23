using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Asserts
{
    public static class AssertAttributeState
    {
        public static async Task<SourceDomain.Attribute> Exists(
            WebApplicationFactory<Program> factory,
            Guid id)
        {
            using var scope = factory.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();

            var attribute = await db.Attributes
                .Include(a => a.Options.OrderBy(o => o.SortOrder))
                .Include(a => a.ApplicableCategories)
                .SingleOrDefaultAsync(a => a.Id == id);

            Assert.NotNull(attribute);
            return attribute;
        }

        public static async Task DoesNotExist(
            WebApplicationFactory<Program> factory,
            Guid id)
        {
            using var scope = factory.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();

            var exists = await db.Attributes.AnyAsync(a => a.Id == id);
            Assert.False(exists, $"Attribute {id} should not exist but was found in DB");
        }

        public static async Task HasVersion(
            WebApplicationFactory<Program> factory,
            Guid id,
            int expectedVersion)
        {
            var attribute = await Exists(factory, id);
            Assert.Equal(expectedVersion, attribute.Version);
        }

        public static async Task HasStatus(
            WebApplicationFactory<Program> factory,
            Guid id,
            SourceDomain.AttributeStatus expectedStatus)
        {
            var attribute = await Exists(factory, id);
            Assert.Equal(expectedStatus, attribute.Status);
        }

        public static async Task HasMetadata(
            WebApplicationFactory<Program> factory,
            Guid id,
            string expectedArabicDisplayName,
            string expectedEnglishDisplayName,
            string? expectedArabicDescription,
            string? expectedEnglishDescription)
        {
            var attribute = await Exists(factory, id);
            Assert.Equal(expectedArabicDisplayName, attribute.ArabicDisplayName);
            Assert.Equal(expectedEnglishDisplayName, attribute.EnglishDisplayName);
            Assert.Equal(expectedArabicDescription, attribute.ArabicDescription);
            Assert.Equal(expectedEnglishDescription, attribute.EnglishDescription);
        }

        public static async Task HasOptions(
            WebApplicationFactory<Program> factory,
            Guid id,
            int expectedCount)
        {
            var attribute = await Exists(factory, id);
            Assert.Equal(expectedCount, attribute.Options.Count);
        }

        public static async Task HasCategories(
            WebApplicationFactory<Program> factory,
            Guid id,
            params int[] expectedCategoryIds)
        {
            var attribute = await Exists(factory, id);
            Assert.Equal(expectedCategoryIds.Length, attribute.ApplicableCategories.Count);

            foreach (var categoryId in expectedCategoryIds)
            {
                Assert.Contains(attribute.ApplicableCategories,
                    c => c.CategoryId == categoryId);
            }
        }

        public static void HasOptionWithSortOrder(
            SourceDomain.Attribute attribute,
            string key,
            int expectedSortOrder)
        {
            var option = Assert.Single(attribute.Options, o => o.Key == key);
            Assert.Equal(expectedSortOrder, option.SortOrder);
        }
    }
}
