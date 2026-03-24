namespace AnisShop.Attributes.Queries.Events;

public record AttributeOptionRemoved : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required string Key { get; init; }
    }
}
