using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeTypeChangedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string? _type;

        public AttributeTypeChangedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeTypeChangedEventFaker WithType(string type)
        {
            _type = type;
            return this;
        }

        public AttributeTypeChanged Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeTypeChanged.EventData
            {
                Type = _type ?? _faker.PickRandom("SingleSelect", "MultiSelect")
            }
        };
    }
}
