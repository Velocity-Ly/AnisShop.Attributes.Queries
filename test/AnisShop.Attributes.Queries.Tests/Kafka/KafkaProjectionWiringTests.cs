using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Events;
using AnisShop.Attributes.Queries.Tests.Helpers;
using AnisShop.Kafka.Sessions;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.Kafka
{
    // Session grouping, ordering, blocking and offset safety belong to AnisShop.Kafka.Sessions and
    // are covered by that package's own suite. What is ours is KafkaEventListener's handler — the
    // deserialize-and-project step — so this drives it through a real partition worker and checks
    // the events come out the far end in the read model.
    public class KafkaProjectionWiringTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;

        public KafkaProjectionWiringTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();

                // appsettings selects Service Bus, so the envelope reader is wired up here. The
                // listener itself is built per test, because its constructor subscribes to a
                // processor and only one handler may be registered on one.
                services.AddSingleton<IKafkaEventDeserializer, KafkaEventDeserializer>();
            });
        }

        [Fact]
        public async Task Project_InterleavedSessions_ReachTheReadModelInPublishOrder()
        {
            // Arrange: two aggregates publishing concurrently, so the partition holds A1, B1, A2...
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
            await using var harness = new PartitionProcessorHarness(log, Listener().ProcessSessionAsync);

            // Act
            harness.Enqueue(log.AppendInterleaved(firstEvents, secondEvents));
            await harness.WaitForPosition(log.NextOffset);

            // Assert: the display name proves the order — V3 overwrote V1's, so seeing V3's value
            // means the events were applied in publish order and not as they sat in the partition.
            var firstAttribute = await AssertAttributeState.Exists(_factory, first.AggregateId);
            Assert.Equal(3, firstAttribute.Version);
            Assert.Equal("Arabic First V3", firstAttribute.ArabicDisplayName);
            Assert.Equal("First V3", firstAttribute.EnglishDisplayName);

            await AssertAttributeState.HasVersion(_factory, second.AggregateId, 3);
            await AssertAttributeState.HasStatus(_factory, second.AggregateId, AttributeStatus.Published);
            await AssertAttributeState.HasCategories(_factory, second.AggregateId, 10, 20);
        }

        [Fact]
        public async Task Project_UnknownEventType_BlocksRatherThanSkipping()
        {
            // Arrange: the deserializer returns null for a type it does not know. Skipping would
            // leave a hole in the read model nothing downstream could detect, so the listener turns
            // that null into a throw and the partition stops.
            var behind = new EventHistoryBuilder();

            var log = new KafkaPartitionLog();
            await using var harness = new PartitionProcessorHarness(log, Listener().ProcessSessionAsync);

            // Act
            harness.Enqueue(log.AppendUnknownType(Guid.NewGuid()));
            await harness.WaitUntil(
                () => harness.StoredPosition == 0, "the partition to block at offset 0");
            await harness.Settle();

            harness.Enqueue(log.Append(behind.Created("Arabic Behind", "Behind", "SingleSelect").Build()));
            await harness.Settle();

            // Assert: nothing behind it consumed, and the cursor still points at the poison record
            Assert.Equal(0, harness.StoredPosition);
            await AssertAttributeState.DoesNotExist(_factory, behind.AggregateId);
        }

        [Fact]
        public async Task Project_VersionGap_BlocksBecauseThePublisherOrderWasViolated()
        {
            // Arrange: an event at V3 for an aggregate the read model has never seen. Under the
            // ordering the publisher promises this cannot happen — a session's events arrive in
            // publish order — so it is a broken promise, and it has to be loud rather than skipped.
            var stuck = new EventHistoryBuilder();
            var orphan = new AttributeMetadataChangedEventFaker()
                .ForAggregate(stuck.AggregateId, version: 3)
                .WithMetadata("Arabic Orphan", "Orphan")
                .Generate();

            var log = new KafkaPartitionLog();
            await using var harness = new PartitionProcessorHarness(log, Listener().ProcessSessionAsync);

            // Act
            var orphanRecord = log.Append(orphan);
            harness.Enqueue(orphanRecord);

            await harness.WaitUntil(
                () => harness.StoredPosition == orphanRecord.Offset.Value,
                "the cursor to stop at the offending message");
            await harness.Settle();

            // Assert: nothing projected, and the cursor never moved past it
            await AssertAttributeState.DoesNotExist(_factory, stuck.AggregateId);
            Assert.Equal(orphanRecord.Offset.Value, harness.StoredPosition);
        }

        // A fresh listener per test: its constructor subscribes to the processor, and only one
        // handler may be registered.
        private KafkaEventListener Listener() =>
            ActivatorUtilities.CreateInstance<KafkaEventListener>(
                _factory.Services,
                new KafkaSessionProcessor(
                    Options.Create(new KafkaSessionProcessorOptions
                    {
                        BootstrapServers = "unused",
                        Topic = "attributes-events",
                        ConsumerGroup = "tests",
                    }),
                    NullLoggerFactory.Instance));
    }
}
