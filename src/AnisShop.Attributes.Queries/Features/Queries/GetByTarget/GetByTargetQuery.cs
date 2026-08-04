using AnisShop.Attributes.Queries.Domain;
using Mediator;

namespace AnisShop.Attributes.Queries.Features.Queries.GetByTarget
{
    public class GetByTargetQuery : IRequest<GetByTargetResult>
    {
        public required AttributeScope Scope { get; init; }
        public required int TargetId { get; init; }
        public required int CurrentPage { get; init; }
        public required int PageSize { get; init; }
    }
}
