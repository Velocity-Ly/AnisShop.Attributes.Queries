using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.Fakers.Events;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.EventsHandler
{
    // One isolated happy-path test per projector that the sequence/lifecycle suites only
    // exercise inside a larger batch. Each test seeds a known precondition, sends the single
    // event under test, and asserts exactly that projector's mutation plus the version bump —
    // so a failure points at one projector, not a whole sequence.
    public class ProjectorHappyPathTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly MediatorHelper _mediatorHelper;
        private readonly DatabaseHelper _databaseHelper;

        public ProjectorHappyPathTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _mediatorHelper = new MediatorHelper(_factory);
            _databaseHelper = new DatabaseHelper(_factory);
        }

        [Fact]
        public async Task Handle_Published_SetsStatusToPublished()
        {
            // Arrange: seed a Draft attribute at V1
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithStatus(SourceDomain.AttributeStatus.Draft)
                    .WithVersion(1));

            var @event = new AttributePublishedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(2, updated.Version);
            Assert.Equal(SourceDomain.AttributeStatus.Published, updated.Status);
        }

        [Fact]
        public async Task Handle_TypeChanged_UpdatesType()
        {
            // Arrange: seed a SingleSelect attribute at V1
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithType(SourceDomain.AttributeType.SingleSelect)
                    .WithVersion(1));

            var @event = new AttributeTypeChangedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .WithType("MultiSelect")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(2, updated.Version);
            Assert.Equal(SourceDomain.AttributeType.MultiSelect, updated.Type);
        }

        [Fact]
        public async Task Handle_MarkedAsDeprecated_SetsStatusAndWarning()
        {
            // Arrange: seed a Published attribute at V1
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithStatus(SourceDomain.AttributeStatus.Published)
                    .WithVersion(1));

            var @event = new AttributeMarkedAsDeprecatedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .WithWarning("Arabic Deprecation Warning", "Deprecation Warning")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(2, updated.Version);
            Assert.Equal(SourceDomain.AttributeStatus.Deprecated, updated.Status);
            Assert.Equal("Arabic Deprecation Warning", updated.ArabicDeprecationWarning);
            Assert.Equal("Deprecation Warning", updated.EnglishDeprecationWarning);
        }

        [Fact]
        public async Task Handle_DeprecationWarningRemoved_PublishesAndClearsWarning()
        {
            // Arrange: seed a Deprecated attribute at V1 (the faker populates the warning fields)
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithStatus(SourceDomain.AttributeStatus.Deprecated)
                    .WithVersion(1));

            Assert.NotNull(attribute.ArabicDeprecationWarning);
            Assert.NotNull(attribute.EnglishDeprecationWarning);

            var @event = new AttributeDeprecationWarningRemovedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(2, updated.Version);
            Assert.Equal(SourceDomain.AttributeStatus.Published, updated.Status);
            Assert.Null(updated.ArabicDeprecationWarning);
            Assert.Null(updated.EnglishDeprecationWarning);
        }

        [Fact]
        public async Task Handle_Disabled_SetsStatusAndReason()
        {
            // Arrange: seed a Published attribute at V1
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithStatus(SourceDomain.AttributeStatus.Published)
                    .WithVersion(1));

            var @event = new AttributeDisabledEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .WithReason("Arabic Disable Reason", "Disable Reason")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(2, updated.Version);
            Assert.Equal(SourceDomain.AttributeStatus.Disabled, updated.Status);
            Assert.Equal("Arabic Disable Reason", updated.ArabicDisableReason);
            Assert.Equal("Disable Reason", updated.EnglishDisableReason);
        }

        [Fact]
        public async Task Handle_Deleted_WithOptionsAndCategories_CascadeDeletesChildren()
        {
            // Arrange: seed an attribute at V1 carrying both options and categories
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithOptions(3)
                    .WithCategoryIds(10, 20)
                    .WithVersion(1));

            var @event = new AttributeDeletedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert: the attribute and ALL child rows are gone
            Assert.True(result);
            await AssertAttributeState.DoesNotExist(_factory, attribute.Id);

            var optionCount = await _databaseHelper.Query(db =>
                db.AttributeOptions.CountAsync(o => o.AttributeId == attribute.Id));
            var categoryCount = await _databaseHelper.Query(db =>
                db.AttributeCategories.CountAsync(c => c.AttributeId == attribute.Id));

            Assert.Equal(0, optionCount);
            Assert.Equal(0, categoryCount);
        }

        [Fact]
        public async Task Handle_CategoriesAdded_InsertsCategories()
        {
            // Arrange: seed an attribute at V1 with no categories
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithVersion(1));

            var @event = new AttributeApplicableCategoriesAddedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .WithCategoryIds(10, 20)
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);
            await AssertAttributeState.HasVersion(_factory, attribute.Id, 2);
            await AssertAttributeState.HasCategories(_factory, attribute.Id, 10, 20);
        }

        [Fact]
        public async Task Handle_CategoriesRemoved_DeletesOnlyRequestedCategories()
        {
            // Arrange: seed an attribute at V1 with three categories
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker()
                    .WithCategoryIds(10, 20, 30)
                    .WithVersion(1));

            var @event = new AttributeApplicableCategoriesRemovedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .WithCategoryIds(20)
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert: only category 20 removed
            Assert.True(result);
            await AssertAttributeState.HasVersion(_factory, attribute.Id, 2);
            await AssertAttributeState.HasCategories(_factory, attribute.Id, 10, 30);
        }

        [Fact]
        public async Task Handle_OptionAdded_InsertsOptionWithSortOrderZero()
        {
            // Arrange: seed an attribute at V1 with no options
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithVersion(1));

            var @event = new AttributeOptionAddedEventFaker()
                .ForAggregate(attribute.Id, version: 2)
                .WithOption("opt-1", "Arabic Label", "Label")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, attribute.Id);
            Assert.Equal(2, updated.Version);

            var option = Assert.Single(updated.Options);
            Assert.Equal("opt-1", option.Key);
            Assert.Equal("Arabic Label", option.ArabicLabel);
            Assert.Equal("Label", option.EnglishLabel);
            Assert.Equal(0, option.SortOrder);
            Assert.False(option.IsDisabled);
        }

        [Fact]
        public async Task Handle_OptionLabelChanged_UpdatesLabels()
        {
            // Arrange: build an attribute with one option (V1-V2) via the event history
            var builder = new EventHistoryBuilder()
                .Created("Arabic Colors", "Colors", "SingleSelect")
                .OptionAdded("color-red", "Arabic Red", "Red");
            var setup = await _mediatorHelper.SendEvents(builder.BuildUpTo(2));
            Assert.True(setup);

            var @event = new AttributeOptionLabelChangedEventFaker()
                .ForAggregate(builder.AggregateId, version: 3)
                .WithOption("color-red", "Arabic Dark Red", "Dark Red")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(3, updated.Version);

            var option = Assert.Single(updated.Options, o => o.Key == "color-red");
            Assert.Equal("Arabic Dark Red", option.ArabicLabel);
            Assert.Equal("Dark Red", option.EnglishLabel);
        }

        [Fact]
        public async Task Handle_OptionDisabled_SetsIsDisabledTrue()
        {
            // Arrange: build an attribute with one (enabled) option via the event history
            var builder = new EventHistoryBuilder()
                .Created("Arabic Sizes", "Sizes", "SingleSelect")
                .OptionAdded("size-m", "Arabic Medium", "Medium");
            await _mediatorHelper.SendEvents(builder.BuildUpTo(2));

            var @event = new AttributeOptionDisabledEventFaker()
                .ForAggregate(builder.AggregateId, version: 3)
                .WithKey("size-m")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(3, updated.Version);

            var option = Assert.Single(updated.Options, o => o.Key == "size-m");
            Assert.True(option.IsDisabled);
        }

        [Fact]
        public async Task Handle_OptionRemoved_DeletesOnlyRequestedOption()
        {
            // Arrange: build an attribute with two options (V1-V3) via the event history
            var builder = new EventHistoryBuilder()
                .Created("Arabic Sizes", "Sizes", "SingleSelect")
                .OptionAdded("size-s", "Arabic Small", "Small")
                .OptionAdded("size-l", "Arabic Large", "Large");
            await _mediatorHelper.SendEvents(builder.BuildUpTo(3));

            var @event = new AttributeOptionRemovedEventFaker()
                .ForAggregate(builder.AggregateId, version: 4)
                .WithKey("size-s")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert: only size-s removed
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(4, updated.Version);

            var option = Assert.Single(updated.Options);
            Assert.Equal("size-l", option.Key);
        }

        [Fact]
        public async Task Handle_OptionsReordered_UpdatesSortOrderByArrayIndex()
        {
            // Arrange: build an attribute with three options (V1-V4) via the event history
            var builder = new EventHistoryBuilder()
                .Created("Arabic Colors", "Colors", "SingleSelect")
                .OptionAdded("color-red", "Arabic Red", "Red")
                .OptionAdded("color-blue", "Arabic Blue", "Blue")
                .OptionAdded("color-green", "Arabic Green", "Green");
            await _mediatorHelper.SendEvents(builder.BuildUpTo(4));

            var @event = new AttributeOptionsReorderedEventFaker()
                .ForAggregate(builder.AggregateId, version: 5)
                .WithOrderedKeys("color-green", "color-red", "color-blue")
                .Generate();

            // Act
            var result = await _mediatorHelper.SendEvents(@event);

            // Assert: array index becomes the 0-based SortOrder
            Assert.True(result);

            var updated = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(5, updated.Version);
            AssertAttributeState.HasOptionWithSortOrder(updated, "color-green", 0);
            AssertAttributeState.HasOptionWithSortOrder(updated, "color-red", 1);
            AssertAttributeState.HasOptionWithSortOrder(updated, "color-blue", 2);
        }
    }
}
