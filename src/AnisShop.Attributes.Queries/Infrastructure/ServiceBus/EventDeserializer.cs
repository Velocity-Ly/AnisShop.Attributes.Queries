using System.Collections.Frozen;
using System.Text.Json;
using AnisShop.Attributes.Queries.Events;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public class EventDeserializer : IEventDeserializer
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
        [EventTypeNames.AttributeApplicableTargetsAdded] = typeof(AttributeApplicableTargetsAdded),
        [EventTypeNames.AttributeApplicableTargetsRemoved] = typeof(AttributeApplicableTargetsRemoved),
    }.ToFrozenDictionary();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly ILogger<EventDeserializer> _logger;

    public EventDeserializer(ILogger<EventDeserializer> logger)
    {
        _logger = logger;
    }

    public EventBase? Deserialize(ServiceBusReceivedMessage message)
    {
        var typeName = message.Subject
            ?? message.ApplicationProperties.GetValueOrDefault("Type") as string;

        if (typeName is null || !TypeMap.TryGetValue(typeName, out var eventType))
        {
            _logger.LogWarning("Unknown event type: {TypeName}", typeName);
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize(message.Body, eventType, JsonOptions) as EventBase;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize event of type {TypeName}", typeName);
            return null;
        }
    }
}
