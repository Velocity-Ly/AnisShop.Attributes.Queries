using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeOptionRemovedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string? _key;

        public AttributeOptionRemovedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeOptionRemovedEventFaker WithKey(string key)
        {
            _key = key;
            return this;
        }

        public AttributeOptionRemoved Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeOptionRemoved.EventData
            {
                Key = _key ?? _faker.Random.AlphaNumeric(10)
            }
        };
    }
}
