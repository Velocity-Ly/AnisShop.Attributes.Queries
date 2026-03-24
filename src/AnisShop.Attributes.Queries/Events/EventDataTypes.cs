namespace AnisShop.Attributes.Queries.Events;

public record AttributeMetadataData
{
    public required string ArabicDisplayName { get; init; }
    public required string EnglishDisplayName { get; init; }
    public string? ArabicDescription { get; init; }
    public string? EnglishDescription { get; init; }
}

public record AttributeOptionData
{
    public required string Key { get; init; }
    public required BilingualTextData Label { get; init; }
}

public record BilingualTextData
{
    public required string Arabic { get; init; }
    public required string English { get; init; }
}
