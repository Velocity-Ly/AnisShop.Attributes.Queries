using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.Fakers.Events;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.EventsHandler
{
    public class AttributeMetadataChangedProjectionTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly MediatorHelper _mediatorHelper;
        private readonly DatabaseHelper _databaseHelper;

        public AttributeMetadataChangedProjectionTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _mediatorHelper = new MediatorHelper(_factory);
            _databaseHelper = new DatabaseHelper(_factory);
        }

        [Fact]
        public async Task Handle_MetadataChanged_UpdatesAllMetadataFields()
        {
            // Arrange: seed an attribute at Version 3
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithStatus(SourceDomain.AttributeStatus.Published)
                    .WithVersion(3));

            var @event = new AttributeMetadataChangedEventFaker()
                .ForAggregate(attribute.Id, version: 4)
                .WithMetadata(
                    "Arabic Updated Name",
                    "Updated Name",
                    "Arabic Updated Description",
                    "Updated Description")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(4, updated.Version);
            Assert.Equal("Arabic Updated Name", updated.ArabicDisplayName);
            Assert.Equal("Updated Name", updated.EnglishDisplayName);
            Assert.Equal("Arabic Updated Description", updated.ArabicDescription);
            Assert.Equal("Updated Description", updated.EnglishDescription);
            Assert.Equal(attribute.Status, updated.Status);
            Assert.Equal(attribute.Type, updated.Type);
        }

        [Fact]
        public async Task Handle_MetadataChanged_WhenVersionAlreadyProcessed_ReturnsTrue_NoChange()
        {
            // Arrange: seed at Version 5
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithVersion(5));

            var @event = new AttributeMetadataChangedEventFaker()
                .ForAggregate(attribute.Id, version: 3)
                .WithMetadata("Arabic Should Not Apply", "Should Not Apply")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert: DB unchanged
            Assert.True(result);

            var unchanged = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(5, unchanged.Version);
            Assert.Equal(attribute.ArabicDisplayName, unchanged.ArabicDisplayName);
            Assert.Equal(attribute.EnglishDisplayName, unchanged.EnglishDisplayName);
        }

        [Fact]
        public async Task Handle_MetadataChanged_WhenVersionGapExists_ReturnsFalse()
        {
            // Arrange: seed at Version 3, send event at Version 5 (gap — missing V4)
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithVersion(3));

            var @event = new AttributeMetadataChangedEventFaker()
                .ForAggregate(attribute.Id, version: 5)
                .WithMetadata("Arabic Should Not Apply", "Should Not Apply")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.False(result);

            var unchanged = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(3, unchanged.Version);
            Assert.Equal(attribute.ArabicDisplayName, unchanged.ArabicDisplayName);
        }
    }
}
