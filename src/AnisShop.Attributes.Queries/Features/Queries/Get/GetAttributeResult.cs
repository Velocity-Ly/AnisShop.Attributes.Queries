using AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Features.Queries.Get
{
    public class GetAttributeResult
    {
        public required Guid Id { get; init; }
        public required string ArabicDisplayName { get; init; }
        public required string EnglishDisplayName { get; init; }
        public required string? ArabicDescription { get; init; }
        public required string? EnglishDescription { get; init; }
        public required AttributeType Type { get; init; }
        public required AttributeStatus Status { get; init; }
        public required string? ArabicDeprecationWarning { get; init; }
        public required string? EnglishDeprecationWarning { get; init; }
        public required string? ArabicDisableReason { get; init; }
        public required string? EnglishDisableReason { get; init; }
        public required int Version { get; init; }
        public required IEnumerable<AttributeOptionItem> Options { get; init; }
        public required IEnumerable<int> ApplicableCategoryIds { get; init; }
    }

    public class AttributeOptionItem
    {
        public required string Key { get; init; }
        public required string ArabicLabel { get; init; }
        public required string EnglishLabel { get; init; }
        public required bool IsDisabled { get; init; }
    }
}
