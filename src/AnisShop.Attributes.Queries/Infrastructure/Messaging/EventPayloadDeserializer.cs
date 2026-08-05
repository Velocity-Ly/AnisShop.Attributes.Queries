using System.Collections.Frozen;
using System.Text.Json;
using AnisShop.Attributes.Queries.Events;

namespace AnisShop.Attributes.Queries.Infrastructure.Messaging;

// The event type map and JSON contract are a property of the *publisher* (Commands), not of the
// broker that carries the bytes. Both the Service Bus deserializer and the Kafka decoder derive
// from this so a new event type is registered in exactly one place; each subclass only knows how
// to pull the type name and the body out of its own message shape.
public abstract class EventPayloadDeserializer
{
    private static readonly FrozenDictionary<string, Type> TypeMap = new Dictionary<string, Type>
    {
        [EventTypeNames.AttributeCreated] = typeof(AttributeCreated),
        [EventTypeNames.AttributePublished] = typeof(AttributePublished),
        [EventTypeNames.AttributeMetadataChanged] = typeof(AttributeMetadataChanged),
        [EventTypeNames.AttributeTypeChanged] = typeof(AttributeTypeChanged),
        [EventTypeNames.AttributeDeleted] = typeof(AttributeDeleted),
        [EventTypeNames.AttributeMarkedAsDeprecated] = typeof(AttributeMarkedAsDeprecated),
        [EventTypeNames.AttributeDeprecationWarningRemoved] = typeof(AttributeDeprecationWarningRemoved),
        [EventTypeNames.AttributeDisabled] = typeof(AttributeDisabled),
        [EventTypeNames.AttributeOptionAdded] = typeof(AttributeOptionAdded),
        [EventTypeNames.AttributeOptionRemoved] = typeof(AttributeOptionRemoved),
        [EventTypeNames.AttributeOptionDisabled] = typeof(AttributeOptionDisabled),
        [EventTypeNames.AttributeOptionLabelChanged] = typeof(AttributeOptionLabelChanged),
        [EventTypeNames.AttributeOptionsReordered] = typeof(AttributeOptionsReordered),
        [EventTypeNames.AttributeApplicableCategoriesAdded] = typeof(AttributeApplicableCategoriesAdded),
        [EventTypeNames.AttributeApplicableCategoriesRemoved] = typeof(AttributeApplicableCategoriesRemoved),
    }.ToFrozenDictionary();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly ILogger _logger;

    protected EventPayloadDeserializer(ILogger logger)
    {
        _logger = logger;
    }

    // Returns null — never throws — so a payload we cannot read is a decision for the caller
    // rather than an unhandled exception on a listener thread.
    protected EventBase? DeserializePayload(string? typeName, ReadOnlyMemory<byte> body)
    {
        if (typeName is null || !TypeMap.TryGetValue(typeName, out var eventType))
        {
            _logger.LogWarning("Unknown event type: {TypeName}", typeName);
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize(body.Span, eventType, JsonOptions) as EventBase;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize event of type {TypeName}", typeName);
            return null;
        }
    }
}
