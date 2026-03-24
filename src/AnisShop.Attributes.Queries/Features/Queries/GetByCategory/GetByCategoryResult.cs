using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Features.Queries.Get;

namespace AnisShop.Attributes.Queries.Features.Queries.GetByCategory
{
    public class GetByCategoryResult
    {
        public required IEnumerable<AttributeItem> Attributes { get; init; }
        public required int CurrentPage { get; init; }
        public required int PageSize { get; init; }
    }

    public class AttributeItem
    {
        public required Guid Id { get; init; }
        public required string ArabicDisplayName { get; init; }
        public required string EnglishDisplayName { get; init; }
        public required string? ArabicDescription { get; init; }
        public required string? EnglishDescription { get; init; }
        public required AttributeType Type { get; init; }
        public required AttributeStatus Status { get; init; }
        public required int Version { get; init; }
        public required IEnumerable<AttributeOptionItem> Options { get; init; }
        public required IEnumerable<int> ApplicableCategoryIds { get; init; }
    }
}
