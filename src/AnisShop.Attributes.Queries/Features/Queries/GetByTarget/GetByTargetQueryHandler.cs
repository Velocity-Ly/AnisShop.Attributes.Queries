using AnisShop.Attributes.Queries.Features.Queries.Get;
using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Mediator;
using Microsoft.EntityFrameworkCore;

namespace AnisShop.Attributes.Queries.Features.Queries.GetByTarget
{
    public class GetByTargetQueryHandler(AttributesDbContext context)
        : IRequestHandler<GetByTargetQuery, GetByTargetResult>
    {
        public async ValueTask<GetByTargetResult> Handle(GetByTargetQuery request, CancellationToken cancellationToken)
        {
            var query = context.Attributes
                .Include(a => a.Options.OrderBy(o => o.SortOrder))
                .Include(a => a.ApplicableTargets)
                .Where(a => a.Scope == request.Scope
                    && a.ApplicableTargets.Any(t => t.TargetId == request.TargetId));

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
                    Scope = a.Scope,
                    Status = a.Status,
                    ArabicDeprecationWarning = a.ArabicDeprecationWarning,
                    EnglishDeprecationWarning = a.EnglishDeprecationWarning,
                    ArabicDisableReason = a.ArabicDisableReason,
                    EnglishDisableReason = a.EnglishDisableReason,
                    Version = a.Version,
                    Options = a.Options.OrderBy(o => o.SortOrder).Select(o => new AttributeOptionItem
                    {
                        Key = o.Key,
                        ArabicLabel = o.ArabicLabel,
                        EnglishLabel = o.EnglishLabel,
                        IsDisabled = o.IsDisabled,
                    }).ToList(),
                    ApplicableTargetIds = a.ApplicableTargets.Select(t => t.TargetId).ToList(),
                })
                .ToListAsync(cancellationToken);

            return new GetByTargetResult
            {
                Attributes = attributes,
                CurrentPage = request.CurrentPage,
                PageSize = request.PageSize,
            };
        }
    }
}
