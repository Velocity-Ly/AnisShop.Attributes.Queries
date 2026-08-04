using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeApplicableTargetsRemovedEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private int[]? _targetIds;

        public AttributeApplicableTargetsRemovedEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeApplicableTargetsRemovedEventFaker WithTargetIds(params int[] targetIds)
        {
            _targetIds = targetIds;
            return this;
        }

        public AttributeApplicableTargetsRemoved Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeApplicableTargetsRemoved.EventData
            {
                ApplicableTargetIds = _targetIds ?? [_faker.Random.Int(1, 1000)]
            }
        };
    }
}
