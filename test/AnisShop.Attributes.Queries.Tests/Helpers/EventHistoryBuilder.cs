using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Tests.Fakers.Events;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public class EventHistoryBuilder
    {
        private readonly Guid _aggregateId;
        private readonly List<EventBase> _events = [];
        private int _nextVersion = 1;

        public EventHistoryBuilder(Guid? aggregateId = null)
        {
            _aggregateId = aggregateId ?? Guid.NewGuid();
        }

        public Guid AggregateId => _aggregateId;

        public EventHistoryBuilder Created(
            string? arabicDisplayName = null,
            string? englishDisplayName = null,
            string? type = null,
            string? scope = null)
        {
            var faker = new AttributeCreatedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion);

            if (arabicDisplayName != null || englishDisplayName != null)
                faker.WithMetadata(
                    arabicDisplayName ?? "Arabic Name",
                    englishDisplayName ?? "English Name");

            if (type != null)
                faker.WithType(type);

            if (scope != null)
                faker.WithScope(scope);

            _events.Add(faker.Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder Published()
        {
            _events.Add(new AttributePublishedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder MetadataChanged(
            string? arabicDisplayName = null,
            string? englishDisplayName = null,
            string? arabicDescription = null,
            string? englishDescription = null)
        {
            var faker = new AttributeMetadataChangedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion);

            if (arabicDisplayName != null || englishDisplayName != null)
                faker.WithMetadata(
                    arabicDisplayName ?? "Arabic Name Updated",
                    englishDisplayName ?? "Updated English Name",
                    arabicDescription,
                    englishDescription);

            _events.Add(faker.Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder TypeChanged(string? type = null)
        {
            var faker = new AttributeTypeChangedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion);

            if (type != null)
                faker.WithType(type);

            _events.Add(faker.Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder MarkedAsDeprecated(string? arabicWarning = null, string? englishWarning = null)
        {
            var faker = new AttributeMarkedAsDeprecatedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion);

            if (arabicWarning != null || englishWarning != null)
                faker.WithWarning(
                    arabicWarning ?? "Arabic Warning",
                    englishWarning ?? "Warning");

            _events.Add(faker.Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder DeprecationWarningRemoved()
        {
            _events.Add(new AttributeDeprecationWarningRemovedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder Disabled(string? arabicReason = null, string? englishReason = null)
        {
            var faker = new AttributeDisabledEventFaker()
                .ForAggregate(_aggregateId, _nextVersion);

            if (arabicReason != null || englishReason != null)
                faker.WithReason(
                    arabicReason ?? "Arabic Disable Reason",
                    englishReason ?? "Disable reason");

            _events.Add(faker.Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder Deleted()
        {
            _events.Add(new AttributeDeletedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder OptionAdded(string? key = null, string? arabicLabel = null, string? englishLabel = null)
        {
            var faker = new AttributeOptionAddedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion);

            if (key != null)
                faker.WithOption(
                    key,
                    arabicLabel ?? "Arabic Label",
                    englishLabel ?? "Label");

            _events.Add(faker.Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder OptionLabelChanged(string key, string arabicLabel, string englishLabel)
        {
            _events.Add(new AttributeOptionLabelChangedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .WithOption(key, arabicLabel, englishLabel)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder OptionDisabled(string key)
        {
            _events.Add(new AttributeOptionDisabledEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .WithKey(key)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder OptionRemoved(string key)
        {
            _events.Add(new AttributeOptionRemovedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .WithKey(key)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder OptionsReordered(params string[] orderedKeys)
        {
            _events.Add(new AttributeOptionsReorderedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .WithOrderedKeys(orderedKeys)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder TargetsAdded(params int[] targetIds)
        {
            _events.Add(new AttributeApplicableTargetsAddedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .WithTargetIds(targetIds)
                .Generate());
            _nextVersion++;
            return this;
        }

        public EventHistoryBuilder TargetsRemoved(params int[] targetIds)
        {
            _events.Add(new AttributeApplicableTargetsRemovedEventFaker()
                .ForAggregate(_aggregateId, _nextVersion)
                .WithTargetIds(targetIds)
                .Generate());
            _nextVersion++;
            return this;
        }

        public List<EventBase> Build() => [.. _events];

        public List<EventBase> BuildFrom(int version)
            => _events.Where(e => e.Version >= version).ToList();

        public List<EventBase> BuildUpTo(int version)
            => _events.Where(e => e.Version <= version).ToList();
    }
}
