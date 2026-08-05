using System.Text;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.Kafka
{
    // Same type map as EventDeserializerTests, different envelope: Kafka has no Subject, so the
    // event type rides in a header. A payload it cannot read comes back as null rather than
    // throwing, and KafkaEventListener turns that null into the throw that blocks the partition.
    public class KafkaEventDeserializerTests
    {
        private readonly KafkaEventDeserializer _deserializer = new(NullLogger<KafkaEventDeserializer>.Instance);

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

            var record = Record(json, headerName, EventTypeNames.AttributeCreated);

            // Act
            var @event = _deserializer.Deserialize(record);

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
            var record = Record("{}", KafkaEventDeserializer.TypeHeader, "SomeUnknownEventType");

            // Act + Assert: reported, not thrown — the partition blocks and the bytes stay put
            Assert.Null(_deserializer.Deserialize(record));
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
            Assert.Null(_deserializer.Deserialize(record));
        }

        [Fact]
        public void Deserialize_MalformedBody_ReturnsNull()
        {
            // Arrange: right type header, unparseable payload
            var record = Record("not json at all", KafkaEventDeserializer.TypeHeader, EventTypeNames.AttributeCreated);

            // Act + Assert
            Assert.Null(_deserializer.Deserialize(record));
        }

        private static ConsumeResult<string, byte[]> Record(string json, string headerName, string typeName) =>
            new()
            {
                Message = new Message<string, byte[]>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Encoding.UTF8.GetBytes(json),
                    Headers = [new Header(headerName, Encoding.UTF8.GetBytes(typeName))],
                },
            };
    }
}
