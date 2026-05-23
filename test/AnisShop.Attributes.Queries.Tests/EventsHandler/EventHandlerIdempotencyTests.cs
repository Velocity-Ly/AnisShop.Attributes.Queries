using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.EventsHandler
{
    public class EventHandlerIdempotencyTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly MediatorHelper _mediatorHelper;
        private readonly DatabaseHelper _databaseHelper;

        public EventHandlerIdempotencyTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _mediatorHelper = new MediatorHelper(_factory);
            _databaseHelper = new DatabaseHelper(_factory);
        }

        [Fact]
        public async Task Handle_FreshProjection_AllEventsApplied()
        {
            // Arrange: no existing attribute, send V1-V3
            var builder = new EventHistoryBuilder();
            var events = builder
                .Created("Arabic Name", "Name", "SingleSelect")
                .Published()
                .MetadataChanged("Arabic Updated Name", "Updated Name")
                .Build();

            // Act
            var result = await _mediatorHelper.SendEvents(events);

            // Assert: all applied, final version = 3
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(3, attribute.Version);
            Assert.Equal(AttributeStatus.Published, attribute.Status);
            Assert.Equal("Arabic Updated Name", attribute.ArabicDisplayName);
            Assert.Equal("Updated Name", attribute.EnglishDisplayName);
        }

        [Fact]
        public async Task Handle_PartialReplay_SkipsAlreadyProcessedEvents_AppliesNewOnes()
        {
            // Arrange: seed attribute at V3 via event replay, then send V1-V5
            var builder = new EventHistoryBuilder();
            var allEvents = builder
                .Created("Arabic Name", "Name", "SingleSelect")
                .Published()
                .MetadataChanged("Arabic Name V3", "Name V3")
                .OptionAdded("opt-1", "Arabic Option 1", "Option 1")
                .CategoriesAdded(10, 20)
                .Build();

            // First, apply V1-V3 to set up the DB state
            var setupEvents = builder.BuildUpTo(3);
            var setupResult = await _mediatorHelper.SendEvents(setupEvents);
            Assert.True(setupResult);
            await AssertAttributeState.HasVersion(_factory, builder.AggregateId, 3);

            // Act: replay V1-V5 (V1-V3 already processed, V4-V5 are new)
            var result = await _mediatorHelper.SendEvents(allEvents);

            // Assert: V4-V5 applied on top of existing V3
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(5, attribute.Version);
            await AssertAttributeState.HasOptions(_factory, builder.AggregateId, 1);
            await AssertAttributeState.HasCategories(_factory, builder.AggregateId, 10, 20);
        }

        [Fact]
        public async Task Handle_FullReplay_AllEventsAlreadyProcessed_ReturnsTrue_NoChange()
        {
            // Arrange: apply V1-V3, then replay V1-V3 again
            var builder = new EventHistoryBuilder();
            var events = builder
                .Created("Arabic Name", "Name", "SingleSelect")
                .Published()
                .MetadataChanged("Arabic Name V3", "Name V3")
                .Build();

            var firstResult = await _mediatorHelper.SendEvents(events);
            Assert.True(firstResult);

            // Act: replay the same events
            var result = await _mediatorHelper.SendEvents(events);

            // Assert: returns true (idempotent success), no change
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(3, attribute.Version);
            Assert.Equal("Arabic Name V3", attribute.ArabicDisplayName);
            Assert.Equal("Name V3", attribute.EnglishDisplayName);
        }

        [Fact]
        public async Task Handle_VersionGap_ReturnsFalse_NoEventsApplied()
        {
            // Arrange: seed attribute at V1, send V3 (missing V2 = gap)
            var builder = new EventHistoryBuilder();
            var createdEvent = builder
                .Created("Arabic Name", "Name", "SingleSelect")
                .Build();

            await _mediatorHelper.SendEvents(createdEvent);

            // Build a V3 event directly (skipping V2)
            var gapBuilder = new EventHistoryBuilder(builder.AggregateId);
            // Need to manually create an event at V3 for this aggregate
            var gapEvent = new Fakers.Events.AttributeMetadataChangedEventFaker()
                .ForAggregate(builder.AggregateId, version: 3)
                .WithMetadata("Arabic Should Not Apply", "Should Not Apply")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(gapEvent);

            // Assert: handler rejects the gap
            Assert.False(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(1, attribute.Version);
            Assert.Equal("Arabic Name", attribute.ArabicDisplayName);
        }
    }
}
