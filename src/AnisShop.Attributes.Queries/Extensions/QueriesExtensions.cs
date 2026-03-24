using AnisShop.Attributes.Queries.Features.Queries.Get;
using AnisShop.Attributes.Queries.Features.Queries.GetByCategory;
using AnisShop.Attributes.Queries.QueriesProto;

namespace AnisShop.Attributes.Queries.Extensions
{
    public static class QueriesExtensions
    {
        extension(GetRequest request)
        {
            public GetAttributeQuery ToQuery() => new()
            {
                Id = Guid.Parse(request.Id),
            };
        }

        extension(GetAttributeResult result)
        {
            public GetResponse ToResponse() => new()
            {
                Attribute = result.ToAttributeOutput(),
            };
        }

        extension(GetAttributeResult result)
        {
            public AttributeOutput ToAttributeOutput() => new()
            {
                Id = result.Id.ToString(),
                ArabicDisplayName = result.ArabicDisplayName,
                EnglishDisplayName = result.EnglishDisplayName,
                ArabicDescription = result.ArabicDescription,
                EnglishDescription = result.EnglishDescription,
                Type = result.Type.ToProtoType(),
                Status = result.Status.ToProtoStatus(),
                Version = result.Version,
                Options = { result.Options.Select(o => o.ToOptionOutput()) },
                ApplicableCategoryIds = { result.ApplicableCategoryIds },
            };
        }

        extension(GetByCategoryRequest request)
        {
            public GetByCategoryQuery ToQuery() => new()
            {
                CategoryId = request.CategoryId,
                CurrentPage = request.CurrentPage,
                PageSize = request.PageSize,
            };
        }

        extension(GetByCategoryResult result)
        {
            public GetByCategoryResponse ToResponse() => new()
            {
                CurrentPage = result.CurrentPage,
                PageSize = result.PageSize,
                Attributes = { result.Attributes.Select(a => a.ToAttributeOutput()) },
            };
        }

        extension(AttributeItem item)
        {
            public AttributeOutput ToAttributeOutput() => new()
            {
                Id = item.Id.ToString(),
                ArabicDisplayName = item.ArabicDisplayName,
                EnglishDisplayName = item.EnglishDisplayName,
                ArabicDescription = item.ArabicDescription,
                EnglishDescription = item.EnglishDescription,
                Type = item.Type.ToProtoType(),
                Status = item.Status.ToProtoStatus(),
                Version = item.Version,
                Options = { item.Options.Select(o => o.ToOptionOutput()) },
                ApplicableCategoryIds = { item.ApplicableCategoryIds },
            };
        }

        extension(AttributeOptionItem option)
        {
            public AttributeOptionOutput ToOptionOutput() => new()
            {
                Key = option.Key,
                ArabicLabel = option.ArabicLabel,
                EnglishLabel = option.EnglishLabel,
                IsDisabled = option.IsDisabled,
            };
        }

        extension(Domain.AttributeType type)
        {
            public AttributeType ToProtoType() => type switch
            {
                Domain.AttributeType.SingleSelect => AttributeType.SingleSelect,
                Domain.AttributeType.MultiSelect => AttributeType.MultiSelect,
                _ => AttributeType.Unspecified,
            };
        }

        extension(Domain.AttributeStatus status)
        {
            public AttributeStatus ToProtoStatus() => status switch
            {
                Domain.AttributeStatus.Draft => AttributeStatus.Draft,
                Domain.AttributeStatus.Published => AttributeStatus.Published,
                Domain.AttributeStatus.Deprecated => AttributeStatus.Deprecated,
                Domain.AttributeStatus.Disabled => AttributeStatus.Disabled,
                _ => AttributeStatus.Unspecified,
            };
        }
    }
}
