namespace AnisShop.Attributes.Queries.Events;

public record AttributeOptionDisabled : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required string Key { get; init; }
    }
}
