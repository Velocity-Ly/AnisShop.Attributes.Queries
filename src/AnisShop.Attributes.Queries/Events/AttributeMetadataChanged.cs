namespace AnisShop.Attributes.Queries.Events;

public record AttributeMetadataChanged : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required AttributeMetadataData Metadata { get; init; }
    }
}
