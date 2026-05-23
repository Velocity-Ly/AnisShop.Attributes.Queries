using AnisShop.Attributes.Queries.Events;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Events
{
    public class AttributeDisabledEventFaker
    {
        private readonly Faker _faker = new();
        private Guid _aggregateId = Guid.NewGuid();
        private int _version = 2;
        private string? _arabicReason;
        private string? _englishReason;

        public AttributeDisabledEventFaker ForAggregate(Guid aggregateId, int version)
        {
            _aggregateId = aggregateId;
            _version = version;
            return this;
        }

        public AttributeDisabledEventFaker WithReason(string arabic, string english)
        {
            _arabicReason = arabic;
            _englishReason = english;
            return this;
        }

        public AttributeDisabled Generate() => new()
        {
            AggregateId = _aggregateId,
            Version = _version,
            UserId = _faker.Random.Guid().ToString(),
            DateTime = DateTime.UtcNow,
            Data = new AttributeDisabled.EventData
            {
                Reason = new BilingualTextData
                {
                    Arabic = _arabicReason ?? _faker.Lorem.Sentence(),
                    English = _englishReason ?? _faker.Lorem.Sentence()
                }
            }
        };
    }
}
