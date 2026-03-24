using Mediator;

namespace AnisShop.Attributes.Queries.Features.Queries.Get
{
    public class GetAttributeQuery : IRequest<GetAttributeResult>
    {
        public required Guid Id { get; init; }
    }
}
