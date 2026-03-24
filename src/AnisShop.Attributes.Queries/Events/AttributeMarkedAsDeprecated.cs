namespace AnisShop.Attributes.Queries.Events;

public record AttributeMarkedAsDeprecated : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required BilingualTextData Warning { get; init; }
    }
}
