using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeMetadataChangedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string? _arabicDisplayName;
        private string? _englishDisplayName;
        private string? _arabicDescription;
        private string? _englishDescription;

        public AttributeMetadataChangedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeMetadataChangedEventFaker WithMetadata(
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

        public AttributeMetadataChanged Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeMetadataChanged.EventData
            {
                Metadata = new AttributeMetadataData
                {
                    ArabicDisplayName = _arabicDisplayName ?? _faker.Commerce.ProductName(),
                    EnglishDisplayName = _englishDisplayName ?? _faker.Commerce.ProductName(),
                    ArabicDescription = _arabicDescription ?? _faker.Lorem.Sentence(),
                    EnglishDescription = _englishDescription ?? _faker.Lorem.Sentence()
                }
            }
        };
    }
}
