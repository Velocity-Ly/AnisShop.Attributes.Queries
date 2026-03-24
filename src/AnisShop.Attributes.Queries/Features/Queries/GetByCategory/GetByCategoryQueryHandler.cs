using AnisShop.Attributes.Queries.Features.Queries.Get;
using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Mediator;
using Microsoft.EntityFrameworkCore;

namespace AnisShop.Attributes.Queries.Features.Queries.GetByCategory
{
    public class GetByCategoryQueryHandler(AttributesDbContext context)
        : IRequestHandler<GetByCategoryQuery, GetByCategoryResult>
    {
        public async ValueTask<GetByCategoryResult> Handle(GetByCategoryQuery request, CancellationToken cancellationToken)
        {
            var query = context.Attributes
                .Include(a => a.Options.OrderBy(o => o.SortOrder))
                .Include(a => a.ApplicableCategories)
                .Where(a => a.ApplicableCategories.Any(c => c.CategoryId == request.CategoryId));

            var attributes = await query
                .Skip((request.CurrentPage - 1) * request.PageSize)
                .Take(request.PageSize)
                .Select(a => new AttributeItem
                {
                    Id = a.Id,
                    ArabicDisplayName = a.ArabicDisplayName,
                    EnglishDisplayName = a.EnglishDisplayName,
                    ArabicDescription = a.ArabicDescription,
                    EnglishDescription = a.EnglishDescription,
                    Type = a.Type,
                    Status = a.Status,
                    Version = a.Version,
                    Options = a.Options.OrderBy(o => o.SortOrder).Select(o => new AttributeOptionItem
                    {
                        Key = o.Key,
                        ArabicLabel = o.ArabicLabel,
                        EnglishLabel = o.EnglishLabel,
                        IsDisabled = o.IsDisabled,
                    }).ToList(),
                    ApplicableCategoryIds = a.ApplicableCategories.Select(c => c.CategoryId).ToList(),
                })
                .ToListAsync(cancellationToken);

            return new GetByCategoryResult
            {
                Attributes = attributes,
                CurrentPage = request.CurrentPage,
                PageSize = request.PageSize,
            };
        }
    }
}
