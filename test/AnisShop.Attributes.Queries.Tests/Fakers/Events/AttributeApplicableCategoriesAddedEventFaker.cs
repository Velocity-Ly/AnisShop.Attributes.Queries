using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeApplicableCategoriesAddedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private int[]? _categoryIds;

        public AttributeApplicableCategoriesAddedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeApplicableCategoriesAddedEventFaker WithCategoryIds(params int[] categoryIds)
        {
            _categoryIds = categoryIds;
            return this;
        }

        public AttributeApplicableCategoriesAdded Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeApplicableCategoriesAdded.EventData
            {
                ApplicableCategoryIds = _categoryIds ?? [_faker.Random.Int(1, 1000), _faker.Random.Int(1, 1000)]
            }
        };
    }
}
