namespace AnisShop.Attributes.Queries.Events;

public record AttributeOptionLabelChanged : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required AttributeOptionData Option { get; init; }
    }
}
