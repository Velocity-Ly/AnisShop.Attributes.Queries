using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.EventsHandler
{
    // Pure unit tests for the message → event mapping. The deserializer is the consumer's
    // first line of defence: an unrecognised or undeserialisable message must be dropped
    // gracefully (logged + null) so a single poison message can never crash the listener.
    public class EventDeserializerTests
    {
        private readonly EventDeserializer _deserializer = new(NullLogger<EventDeserializer>.Instance);

        [Fact]
        public void Deserialize_KnownType_ReturnsTypedEvent()
        {
            // Arrange: a well-formed AttributeCreated body in the camelCase shape Commands publishes
            var aggregateId = Guid.NewGuid();
            var json = $$"""
            {
              "aggregateId": "{{aggregateId}}",
              "version": 1,
              "userId": "user-1",
              "dateTime": "2026-05-23T00:00:00Z",
              "data": {
                "metadata": {
                  "arabicDisplayName": "Arabic Name",
                  "englishDisplayName": "English Name"
                },
                "type": "SingleSelect"
              }
            }
            """;

            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString(json),
                subject: EventTypeNames.AttributeCreated);

            // Act
            var result = _deserializer.Deserialize(message);

            // Assert
            var created = Assert.IsType<AttributeCreated>(result);
            Assert.Equal(aggregateId, created.AggregateId);
            Assert.Equal(1, created.Version);
            Assert.Equal("user-1", created.UserId);
            Assert.Equal("SingleSelect", created.Data.Type);
            Assert.Equal("Arabic Name", created.Data.Metadata.ArabicDisplayName);
            Assert.Equal("English Name", created.Data.Metadata.EnglishDisplayName);
        }

        [Fact]
        public void Deserialize_UnknownType_ReturnsNull()
        {
            // Arrange: a subject that is not in the type map
            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("{}"),
                subject: "SomeUnknownEventType");

            // Act
            var result = _deserializer.Deserialize(message);

            // Assert: dropped gracefully, no throw
            Assert.Null(result);
        }

        [Fact]
        public void Deserialize_MissingType_ReturnsNull()
        {
            // Arrange: no subject and no "Type" application property
            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("{}"));

            // Act
            var result = _deserializer.Deserialize(message);

            // Assert
            Assert.Null(result);
        }
    }
}
