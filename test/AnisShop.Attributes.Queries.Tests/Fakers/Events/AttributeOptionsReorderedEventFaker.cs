using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeOptionsReorderedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string[]? _orderedKeys;

        public AttributeOptionsReorderedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeOptionsReorderedEventFaker WithOrderedKeys(params string[] keys)
        {
            _orderedKeys = keys;
            return this;
        }

        public AttributeOptionsReordered Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeOptionsReordered.EventData
            {
                OrderedKeys = _orderedKeys ?? [_faker.Random.AlphaNumeric(10), _faker.Random.AlphaNumeric(10)]
            }
        };
    }
}
