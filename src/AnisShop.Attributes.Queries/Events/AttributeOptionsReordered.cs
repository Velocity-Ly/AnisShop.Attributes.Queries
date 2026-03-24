namespace AnisShop.Attributes.Queries.Events;

public record AttributeOptionsReordered : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required IReadOnlyList<string> OrderedKeys { get; init; }
    }
}
