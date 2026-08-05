using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Events;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.KafkaFlowTransport
{
    // Consuming, worker distribution, buffering and offset management all belong to KafkaFlow, so
    // none of it is tested here. What is ours is one middleware's worth of work: take everything a
    // worker collected, split it back into per-aggregate runs, and project them. These drive that
    // directly — no host, no harness, no waiting, because there is no loop of ours to run.
    public class KafkaFlowProjectionTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;

        public KafkaFlowProjectionTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();

                // appsettings selects Service Bus, so the envelope reader this transport uses is
                // wired up here rather than by AddKafkaFlowListener.
                services.AddSingleton<IKafkaFlowEventDeserializer, KafkaFlowEventDeserializer>();
            });
        }

        [Fact]
        public async Task Project_TwoAggregatesInOneBatch_ReachTheReadModelInPublishOrder()
        {
            // Arrange: two aggregates publishing concurrently. Their keys hashed to the same worker,
            // so one dispatch carries both, interleaved: A1, B1, A2, B2...
            var first = new EventHistoryBuilder();
            var second = new EventHistoryBuilder();

            var firstEvents = first.Created("Arabic First", "First", "SingleSelect")
                .Published()
                .MetadataChanged("Arabic First V3", "First V3")
                .Build();

            var secondEvents = second.Created("Arabic Second", "Second", "MultiSelect")
                .CategoriesAdded(10, 20)
                .Published()
                .Build();

            var log = new KafkaPartitionLog();

            // Act
            await Projector().ProjectAsync(
                KafkaFlowWorkerBatch.Of(log.AppendInterleaved(firstEvents, secondEvents)),
                CancellationToken.None);

            // Assert: the display name proves the order — V3 overwrote V1's, so seeing V3's value
            // means the events were applied in publish order and not as the batch held them.
            var firstAttribute = await AssertAttributeState.Exists(_factory, first.AggregateId);
            Assert.Equal(3, firstAttribute.Version);
            Assert.Equal("Arabic First V3", firstAttribute.ArabicDisplayName);
            Assert.Equal("First V3", firstAttribute.EnglishDisplayName);

            await AssertAttributeState.HasVersion(_factory, second.AggregateId, 3);
            await AssertAttributeState.HasStatus(_factory, second.AggregateId, AttributeStatus.Published);
            await AssertAttributeState.HasCategories(_factory, second.AggregateId, 10, 20);
        }

        [Fact]
        public async Task Project_ReplayedBatch_LeavesTheReadModelUnchanged()
        {
            // Arrange: KafkaFlow stores an offset only once the batch comes back clean, so a
            // rebalance re-delivers whatever was in flight. Nothing deduplicates it.
            var attribute = new EventHistoryBuilder();
            var events = attribute.Created("Arabic Replay", "Replay", "SingleSelect")
                .Published()
                .Build();

            var log = new KafkaPartitionLog();
            var batch = KafkaFlowWorkerBatch.Of(log.Append(events));

            // Act: the same run handed over twice
            await Projector().ProjectAsync(batch, CancellationToken.None);
            await Projector().ProjectAsync(batch, CancellationToken.None);

            // Assert: the second pass was absorbed by the projection, not applied again
            await AssertAttributeState.HasVersion(_factory, attribute.AggregateId, 2);
            await AssertAttributeState.HasStatus(_factory, attribute.AggregateId, AttributeStatus.Published);
        }

        [Fact]
        public async Task Project_UnknownEventType_ThrowsAndAbandonsTheRestOfTheBatch()
        {
            // Arrange: a type the deserializer does not know, with a perfectly healthy aggregate
            // behind it. Skipping the poison message would leave a hole nothing downstream could
            // detect, so the projector throws — and everything else the worker collected goes with
            // it, because KafkaFlow completes the whole batch once this returns.
            var behind = new EventHistoryBuilder();

            var log = new KafkaPartitionLog();
            var batch = KafkaFlowWorkerBatch.Of([
                log.AppendUnknownType(Guid.NewGuid()),
                .. log.Append(behind.Created("Arabic Behind", "Behind", "SingleSelect").Build()),
            ]);

            // Act
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => Projector().ProjectAsync(batch, CancellationToken.None));

            // Assert
            Assert.Contains("cannot be deserialized", exception.Message);
            await AssertAttributeState.DoesNotExist(_factory, behind.AggregateId);
        }

        [Fact]
        public async Task Project_VersionGap_ThrowsBecauseThePublisherOrderWasViolated()
        {
            // Arrange: an event at V3 for an aggregate the read model has never seen. One key never
            // moves between workers, so its events cannot overtake each other — reaching here means
            // the publisher's ordering promise was broken, and it has to be loud.
            var stuck = new EventHistoryBuilder();
            var orphan = new AttributeMetadataChangedEventFaker()
                .ForAggregate(stuck.AggregateId, version: 3)
                .WithMetadata("Arabic Orphan", "Orphan")
                .Generate();

            var log = new KafkaPartitionLog();

            // Act
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => Projector().ProjectAsync(
                    KafkaFlowWorkerBatch.Of(log.Append(orphan)), CancellationToken.None));

            // Assert
            Assert.Contains("not at version 2", exception.Message);
            await AssertAttributeState.DoesNotExist(_factory, stuck.AggregateId);
        }

        private KafkaFlowEventProjector Projector() =>
            ActivatorUtilities.CreateInstance<KafkaFlowEventProjector>(_factory.Services);
    }
}
