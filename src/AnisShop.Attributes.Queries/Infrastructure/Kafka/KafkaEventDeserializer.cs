using System.Text;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Messaging;
using Confluent.Kafka;

namespace AnisShop.Attributes.Queries.Infrastructure.Kafka;

public interface IKafkaEventDeserializer
{
    EventBase? Deserialize(ConsumeResult<string, byte[]> message);
}

// The Kafka envelope reader. The type map and JSON contract are shared with the Service Bus
// reader — they belong to the publisher, not to the broker carrying the bytes.
public class KafkaEventDeserializer : EventPayloadDeserializer, IKafkaEventDeserializer
{
    // Kafka has no Subject field, so the event type travels as a header. Both spellings are
    // accepted for the same reason the Service Bus reader falls back to the "Type" application
    // property: publishers in the wild disagree on the casing.
    public const string TypeHeader = "type";
    public const string LegacyTypeHeader = "Type";

    public KafkaEventDeserializer(ILogger<KafkaEventDeserializer> logger)
        : base(logger)
    {
    }

    public EventBase? Deserialize(ConsumeResult<string, byte[]> message) =>
        DeserializePayload(ReadTypeHeader(message.Message.Headers), message.Message.Value.AsMemory());

    private static string? ReadTypeHeader(Headers? headers)
    {
        if (headers is null)
            return null;

        if (headers.TryGetLastBytes(TypeHeader, out var value)
            || headers.TryGetLastBytes(LegacyTypeHeader, out value))
        {
            return Encoding.UTF8.GetString(value);
        }

        return null;
    }
}
