using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeCreatedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 1;
        private string? _arabicDisplayName;
        private string? _englishDisplayName;
        private string? _arabicDescription;
        private string? _englishDescription;
        private string? _type;
        private string? _scope;

        public AttributeCreatedEventFaker ForAggregate(Guid aggregateId, int version = 1)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeCreatedEventFaker WithMetadata(
            string arabicDisplayName,
            string englishDisplayName,
            string? arabicDescription = null,
            string? englishDescription = null)
        {
            _arabicDisplayName = arabicDisplayName;
            _englishDisplayName = englishDisplayName;
            _arabicDescription = arabicDescription;
            _englishDescription = englishDescription;
            return this;
        }

        public AttributeCreatedEventFaker WithType(string type)
        {
            _type = type;
            return this;
        }

        public AttributeCreatedEventFaker WithScope(string scope)
        {
            _scope = scope;
            return this;
        }

        public AttributeCreated Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeCreated.EventData
            {
                Metadata = new AttributeMetadataData
                {
                    ArabicDisplayName = _arabicDisplayName ?? _faker.Commerce.ProductName(),
                    EnglishDisplayName = _englishDisplayName ?? _faker.Commerce.ProductName(),
                    ArabicDescription = _arabicDescription ?? _faker.Lorem.Sentence(),
                    EnglishDescription = _englishDescription ?? _faker.Lorem.Sentence()
                },
                Type = _type ?? _faker.PickRandom("SingleSelect", "MultiSelect"),
                Scope = _scope ?? _faker.PickRandom("ProductCategory", "MarketType")
            }
        };
    }
}
