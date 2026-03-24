namespace AnisShop.Attributes.Queries.Events;

public abstract record EventBase
{
    public required Guid AggregateId { get; init; }
    public required int Version { get; init; }
    public required string UserId { get; init; }
    public required DateTime DateTime { get; init; }
}
