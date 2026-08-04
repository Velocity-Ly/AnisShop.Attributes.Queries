using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Events;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.EventsHandler
{
    public class AttributeCreatedProjectionTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly MediatorHelper _mediatorHelper;

        public AttributeCreatedProjectionTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _mediatorHelper = new MediatorHelper(_factory);
        }

        [Fact]
        public async Task Handle_AttributeCreated_InsertsAttributeWithDraftStatusAndVersion1()
        {
            // Arrange
            var aggregateId = Guid.NewGuid();
            var @event = new AttributeCreatedEventFaker()
                .ForAggregate(aggregateId)
                .WithMetadata("Arabic Display Name", "Display Name", "Arabic Description", "English Description")
                .WithType("SingleSelect")
                .WithScope("ProductCategory")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, aggregateId);
            Assert.Equal(1, attribute.Version);
            Assert.Equal(AttributeStatus.Draft, attribute.Status);
            Assert.Equal(AttributeType.SingleSelect, attribute.Type);
            Assert.Equal(AttributeScope.ProductCategory, attribute.Scope);
            Assert.Equal("Arabic Display Name", attribute.ArabicDisplayName);
            Assert.Equal("Display Name", attribute.EnglishDisplayName);
            Assert.Equal("Arabic Description", attribute.ArabicDescription);
            Assert.Equal("English Description", attribute.EnglishDescription);
            Assert.Empty(attribute.Options);
            Assert.Empty(attribute.ApplicableTargets);
        }

        [Fact]
        public async Task Handle_AttributeCreated_WhenAttributeAlreadyExists_ReturnsTrue_NoChange()
        {
            // Arrange: seed an existing attribute at Version 1
            var aggregateId = Guid.NewGuid();
            var firstEvent = new AttributeCreatedEventFaker()
                .ForAggregate(aggregateId)
                .WithMetadata("Arabic First Name", "First Name")
                .WithType("SingleSelect")
                .Generate();

            await _mediatorHelper.SendEvents(firstEvent);

            // Act: send another Created event with Version 1 (duplicate)
            var duplicateEvent = new AttributeCreatedEventFaker()
                .ForAggregate(aggregateId)
                .WithMetadata("Arabic Different Name", "Different Name")
                .WithType("MultiSelect")
                .Generate();

            var result = await _mediatorHelper.SendEvents(duplicateEvent);

            // Assert: original data unchanged
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, aggregateId);
            Assert.Equal(1, attribute.Version);
            Assert.Equal("Arabic First Name", attribute.ArabicDisplayName);
            Assert.Equal("First Name", attribute.EnglishDisplayName);
            Assert.Equal(AttributeType.SingleSelect, attribute.Type);
        }
    }
}
