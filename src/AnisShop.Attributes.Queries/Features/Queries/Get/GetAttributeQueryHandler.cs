using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.Resources;
using Grpc.Core;
using Mediator;
using Microsoft.EntityFrameworkCore;

namespace AnisShop.Attributes.Queries.Features.Queries.Get
{
    public class GetAttributeQueryHandler(AttributesDbContext context)
        : IRequestHandler<GetAttributeQuery, GetAttributeResult>
    {
        public async ValueTask<GetAttributeResult> Handle(GetAttributeQuery request, CancellationToken cancellationToken)
        {
            var attribute = await context.Attributes
                .Include(a => a.Options.OrderBy(o => o.SortOrder))
                .Include(a => a.ApplicableTargets)
                .FirstOrDefaultAsync(a => a.Id == request.Id, cancellationToken)
                ?? throw new RpcException(new Status(StatusCode.NotFound, Messages.AttributeNotFound));

            return new GetAttributeResult
            {
                Id = attribute.Id,
                ArabicDisplayName = attribute.ArabicDisplayName,
                EnglishDisplayName = attribute.EnglishDisplayName,
                ArabicDescription = attribute.ArabicDescription,
                EnglishDescription = attribute.EnglishDescription,
                Type = attribute.Type,
                Scope = attribute.Scope,
                Status = attribute.Status,
                ArabicDeprecationWarning = attribute.ArabicDeprecationWarning,
                EnglishDeprecationWarning = attribute.EnglishDeprecationWarning,
                ArabicDisableReason = attribute.ArabicDisableReason,
                EnglishDisableReason = attribute.EnglishDisableReason,
                Version = attribute.Version,
                Options = attribute.Options.Select(o => new AttributeOptionItem
                {
                    Key = o.Key,
                    ArabicLabel = o.ArabicLabel,
                    EnglishLabel = o.EnglishLabel,
                    IsDisabled = o.IsDisabled,
                }),
                ApplicableTargetIds = attribute.ApplicableTargets.Select(t => t.TargetId),
            };
        }
    }
}
