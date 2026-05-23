using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.EventsHandler
{
    public class EventSequenceProjectionTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly MediatorHelper _mediatorHelper;

        public EventSequenceProjectionTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _mediatorHelper = new MediatorHelper(_factory);
        }

        [Fact]
        public async Task Handle_FullAttributeLifecycle_ProducesCorrectFinalState()
        {
            // Created(V1) → Published(V2) → MetadataChanged(V3) → MarkedAsDeprecated(V4)
            // → DeprecationWarningRemoved(V5) → Disabled(V6) → Deleted(V7)
            var history = new EventHistoryBuilder()
                .Created("Arabic Original Name", "Original Name", "SingleSelect")
                .Published()
                .MetadataChanged("Arabic Updated Name", "Updated Name", "Arabic Updated Description", "Updated Description")
                .MarkedAsDeprecated("Arabic Deprecation Warning", "Deprecation Warning")
                .DeprecationWarningRemoved()
                .Disabled("Arabic Disable Reason", "Disable Reason")
                .Deleted()
                .Build();

            // Act
            var result = await _mediatorHelper.SendEvents(history);

            // Assert: attribute should be deleted after full lifecycle
            Assert.True(result);
            await AssertAttributeState.DoesNotExist(_factory, history[0].AggregateId);
        }

        [Fact]
        public async Task Handle_OptionLifecycle_ProducesCorrectFinalState()
        {
            // Created(V1) → OptionAdded "color-red"(V2) → OptionAdded "color-blue"(V3)
            // → OptionAdded "color-green"(V4) → OptionsReordered(V5)
            // → OptionLabelChanged "color-red"(V6) → OptionDisabled "color-blue"(V7)
            // → OptionRemoved "color-green"(V8)
            var builder = new EventHistoryBuilder();
            var history = builder
                .Created("Arabic Colors", "Colors", "SingleSelect")
                .OptionAdded("color-red", "Arabic Red", "Red")
                .OptionAdded("color-blue", "Arabic Blue", "Blue")
                .OptionAdded("color-green", "Arabic Green", "Green")
                .OptionsReordered("color-green", "color-red", "color-blue")
                .OptionLabelChanged("color-red", "Arabic Dark Red", "Dark Red")
                .OptionDisabled("color-blue")
                .OptionRemoved("color-green")
                .Build();

            // Act
            var result = await _mediatorHelper.SendEvents(history);

            // Assert
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(8, attribute.Version);
            Assert.Equal(2, attribute.Options.Count);

            var options = attribute.Options.OrderBy(o => o.SortOrder).ToList();

            // color-red: was reordered to index 1, then label changed
            var red = Assert.Single(attribute.Options, o => o.Key == "color-red");
            Assert.Equal("Arabic Dark Red", red.ArabicLabel);
            Assert.Equal("Dark Red", red.EnglishLabel);
            Assert.False(red.IsDisabled);

            // color-blue: was reordered to index 2, then disabled
            var blue = Assert.Single(attribute.Options, o => o.Key == "color-blue");
            Assert.True(blue.IsDisabled);
        }

        [Fact]
        public async Task Handle_CategoryLifecycle_ProducesCorrectFinalState()
        {
            // Created(V1) → CategoriesAdded [10,20,30](V2) → CategoriesAdded [40,50](V3)
            // → CategoriesRemoved [20,30](V4)
            var builder = new EventHistoryBuilder();
            var history = builder
                .Created("Arabic Categories", "Categories", "MultiSelect")
                .CategoriesAdded(10, 20, 30)
                .CategoriesAdded(40, 50)
                .CategoriesRemoved(20, 30)
                .Build();

            // Act
            var result = await _mediatorHelper.SendEvents(history);

            // Assert
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(4, attribute.Version);
            await AssertAttributeState.HasCategories(_factory, builder.AggregateId, 10, 40, 50);
        }

        [Fact]
        public async Task Handle_MixedBatch_AllEventTypesInSequence_ProducesCorrectFinalState()
        {
            // Created(V1) → OptionAdded(V2) → CategoriesAdded(V3) → Published(V4) → MetadataChanged(V5)
            var builder = new EventHistoryBuilder();
            var history = builder
                .Created("Arabic Original Name", "Original Name", "SingleSelect")
                .OptionAdded("opt-1", "Arabic Option", "Option")
                .CategoriesAdded(100, 200)
                .Published()
                .MetadataChanged("Arabic Final Name", "Final Name", "Arabic Final Description", "Final Description")
                .Build();

            // Act
            var result = await _mediatorHelper.SendEvents(history);

            // Assert
            Assert.True(result);

            var attribute = await AssertAttributeState.Exists(_factory, builder.AggregateId);
            Assert.Equal(5, attribute.Version);
            Assert.Equal(AttributeStatus.Published, attribute.Status);
            Assert.Equal("Arabic Final Name", attribute.ArabicDisplayName);
            Assert.Equal("Final Name", attribute.EnglishDisplayName);
            Assert.Equal("Arabic Final Description", attribute.ArabicDescription);
            Assert.Equal("Final Description", attribute.EnglishDescription);

            await AssertAttributeState.HasOptions(_factory, builder.AggregateId, 1);
            await AssertAttributeState.HasCategories(_factory, builder.AggregateId, 100, 200);
        }
    }
}
