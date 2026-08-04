namespace AnisShop.Attributes.Queries.Events;

public record AttributeApplicableTargetsRemoved : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required IReadOnlyList<int> ApplicableTargetIds { get; init; }
    }
}
