namespace AnisShop.Attributes.Queries.Events;

public record AttributeTypeChanged : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required string Type { get; init; }
    }
}
