using System.Text;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;
using AnisShop.Attributes.Queries.Tests.FakeServices;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.KafkaFlowTransport
{
    // The same envelope KafkaEventDeserializerTests covers, read out of KafkaFlow's message shape
    // instead of a ConsumeResult. Both share one type map, so what is under test here is only the
    // header lookup and the raw-bytes assumption.
    public class KafkaFlowEventDeserializerTests
    {
        private readonly KafkaFlowEventDeserializer _deserializer =
            new(NullLogger<KafkaFlowEventDeserializer>.Instance);

        [Theory]
        [InlineData(KafkaEventDeserializer.TypeHeader)]
        [InlineData(KafkaEventDeserializer.LegacyTypeHeader)]
        public void Deserialize_KnownTypeHeader_ReturnsTypedEvent(string headerName)
        {
            // Arrange: a well-formed AttributeCreated body in the camelCase shape Commands publishes
            var aggregateId = Guid.NewGuid();
            var json = $$"""
            {
              "aggregateId": "{{aggregateId}}",
              "version": 1,
              "userId": "user-1",
              "dateTime": "2026-08-05T00:00:00Z",
              "data": {
                "metadata": {
                  "arabicDisplayName": "Arabic Name",
                  "englishDisplayName": "English Name"
                },
                "type": "SingleSelect"
              }
            }
            """;

            // Act
            var @event = _deserializer.Deserialize(Message(json, headerName, EventTypeNames.AttributeCreated));

            // Assert
            var created = Assert.IsType<AttributeCreated>(@event);
            Assert.Equal(aggregateId, created.AggregateId);
            Assert.Equal(1, created.Version);
            Assert.Equal("user-1", created.UserId);
            Assert.Equal("SingleSelect", created.Data.Type);
            Assert.Equal("Arabic Name", created.Data.Metadata.ArabicDisplayName);
            Assert.Equal("English Name", created.Data.Metadata.EnglishDisplayName);
        }

        [Fact]
        public void Deserialize_UnknownTypeHeader_ReturnsNull()
        {
            // Arrange: a type header that is not in the shared type map
            var message = Message("{}", KafkaEventDeserializer.TypeHeader, "SomeUnknownEventType");

            // Act + Assert: reported rather than thrown — the projector decides what a null means
            Assert.Null(_deserializer.Deserialize(message));
        }

        [Fact]
        public void Deserialize_MissingTypeHeader_ReturnsNull()
        {
            // Arrange: headers present but no type at all
            var record = new ConsumeResult<string, byte[]>
            {
                Message = new Message<string, byte[]>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Encoding.UTF8.GetBytes("{}"),
                    Headers = [],
                },
            };

            // Act + Assert
            Assert.Null(_deserializer.Deserialize(new FakeMessageContext(record)));
        }

        [Fact]
        public void Deserialize_MalformedBody_ReturnsNull()
        {
            // Arrange: right type header, unparseable payload
            var message = Message("not json at all", KafkaEventDeserializer.TypeHeader, EventTypeNames.AttributeCreated);

            // Act + Assert
            Assert.Null(_deserializer.Deserialize(message));
        }

        private static FakeMessageContext Message(string json, string headerName, string typeName) =>
            new(new ConsumeResult<string, byte[]>
            {
                Message = new Message<string, byte[]>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Encoding.UTF8.GetBytes(json),
                    Headers = [new Header(headerName, Encoding.UTF8.GetBytes(typeName))],
                },
            });
    }
}
