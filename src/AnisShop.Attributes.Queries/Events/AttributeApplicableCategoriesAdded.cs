namespace AnisShop.Attributes.Queries.Events;

public record AttributeApplicableCategoriesAdded : EventBase
{
    public required EventData Data { get; init; }

    public record EventData
    {
        public required IReadOnlyList<int> ApplicableCategoryIds { get; init; }
    }
}
