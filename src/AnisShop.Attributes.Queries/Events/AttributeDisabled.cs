namespace AnisShop.Attributes.Queries.Events;

public record AttributeDisabled : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required BilingualTextData Reason { get; init; }
    }
}
