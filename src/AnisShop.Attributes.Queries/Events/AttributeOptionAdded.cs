namespace AnisShop.Attributes.Queries.Events;

public record AttributeOptionAdded : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required AttributeOptionData Option { get; init; }
    }
}
