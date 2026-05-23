using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeOptionLabelChangedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string? _key;
        private string? _arabicLabel;
        private string? _englishLabel;

        public AttributeOptionLabelChangedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeOptionLabelChangedEventFaker WithOption(string key, string arabicLabel, string englishLabel)
        {
            _key = key;
            _arabicLabel = arabicLabel;
            _englishLabel = englishLabel;
            return this;
        }

        public AttributeOptionLabelChanged Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeOptionLabelChanged.EventData
            {
                Option = new AttributeOptionData
                {
                    Key = _key ?? _faker.Random.AlphaNumeric(10),
                    Label = new BilingualTextData
                    {
                        Arabic = _arabicLabel ?? _faker.Commerce.ProductAdjective(),
                        English = _englishLabel ?? _faker.Commerce.ProductAdjective()
                    }
                }
            }
        };
    }
}
