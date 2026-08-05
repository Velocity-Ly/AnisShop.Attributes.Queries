using System.Text;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Kafka;
using AnisShop.Attributes.Queries.Infrastructure.Messaging;
using KafkaFlow;

namespace AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;

public interface IKafkaFlowEventDeserializer
{
    EventBase? Deserialize(IMessageContext message);
}

// The same envelope the hand-rolled Kafka listener reads — event type in a header, event as JSON —
// arriving in KafkaFlow's shape instead of a ConsumeResult. The header names and the type map are
// shared with it, because both belong to the publisher rather than to any one consumer.
public class KafkaFlowEventDeserializer : EventPayloadDeserializer, IKafkaFlowEventDeserializer
{
    public KafkaFlowEventDeserializer(ILogger<KafkaFlowEventDeserializer> logger)
        : base(logger)
    {
    }

    public EventBase? Deserialize(IMessageContext message)
    {
        // No deserializer middleware is registered, so KafkaFlow hands over the broker bytes
        // untouched. Anything else means the pipeline was reconfigured underneath us.
        if (message.Message.Value is not byte[] body)
            return null;

        return DeserializePayload(ReadTypeHeader(message.Headers), body.AsMemory());
    }

    private static string? ReadTypeHeader(IMessageHeaders headers)
    {
        var value = headers[KafkaEventDeserializer.TypeHeader]
            ?? headers[KafkaEventDeserializer.LegacyTypeHeader];

        return value is null ? null : Encoding.UTF8.GetString(value);
    }
}
