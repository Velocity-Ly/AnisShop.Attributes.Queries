using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributePublishedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;

        public AttributePublishedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributePublished Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow
        };
    }
}
