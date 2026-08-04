namespace AnisShop.Attributes.Queries.Events;

public record AttributeCreated : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required AttributeMetadataData Metadata { get; init; }
        public required string Type { get; init; }
        public required string Scope { get; init; }
    }
}
