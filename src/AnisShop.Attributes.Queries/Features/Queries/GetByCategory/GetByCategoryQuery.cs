using Mediator;

namespace AnisShop.Attributes.Queries.Features.Queries.GetByCategory
{
    public class GetByCategoryQuery : IRequest<GetByCategoryResult>
    {
        public required int CategoryId { get; init; }
        public required int CurrentPage { get; init; }
        public required int PageSize { get; init; }
    }
}
