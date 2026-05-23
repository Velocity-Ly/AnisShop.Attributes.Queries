using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeOptionDisabledEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string? _key;

        public AttributeOptionDisabledEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeOptionDisabledEventFaker WithKey(string key)
        {
            _key = key;
            return this;
        }

        public AttributeOptionDisabled Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeOptionDisabled.EventData
            {
                Key = _key ?? _faker.Random.AlphaNumeric(10)
            }
        };
    }
}
