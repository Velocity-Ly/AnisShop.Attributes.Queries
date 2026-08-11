namespace AnisShop.Attributes.Sample.Publisher;

// These records mirror the WIRE CONTRACT the query side expects — the JSON shape and the type name,
// not any shared code. A real command service owns its own event definitions (or a shared contracts
// package); what has to match is what lands on the topic:
//
//   key    = AggregateId as a UTF-8 string
//   header = "type" = the event type's name ("AttributeCreated", …)
//   value  = the event as camelCase JSON
//
// The consumer deserializes by that type header into the matching type with camelCase options, so a
// mismatch in any of the three is what turns a message into a poison message.

public abstract record EventBase
{
    public required Guid AggregateId { get; init; }
    public required int Version { get; init; }
    public required string UserId { get; init; }
    public required DateTime DateTime { get; init; }
}

public record AttributeCreated : EventBase
{
    public required CreatedData Data { get; init; }

    public record CreatedData
    {
        public required MetadataData Metadata { get; init; }
        public required string Type { get; init; } // "SingleSelect" | "MultiSelect"
    }
}

public record AttributePublished : EventBase;

public record AttributeMetadataChanged : EventBase
{
    public required MetadataChangedData Data { get; init; }

    public record MetadataChangedData
    {
        public required MetadataData Metadata { get; init; }
    }
}

public record MetadataData
{
    public required string ArabicDisplayName { get; init; }
    public required string EnglishDisplayName { get; init; }
    public string? ArabicDescription { get; init; }
    public string? EnglishDescription { get; init; }
}
