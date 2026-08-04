using AnisShop.Attributes.Queries.Extensions;
using AnisShop.Attributes.Queries.QueriesProto;
using Grpc.Core;
using Mediator;

namespace AnisShop.Attributes.Queries.GrpcServices
{
    public class AttributesQueriesService(IMediator mediator) : AttributesQueries.AttributesQueriesBase
    {
        public override async Task<GetResponse> Get(GetRequest request, ServerCallContext context)
        {
            var query = request.ToQuery();
            var result = await mediator.Send(query, context.CancellationToken);
            return result.ToResponse();
        }

        public override async Task<GetByTargetResponse> GetByTarget(GetByTargetRequest request, ServerCallContext context)
        {
            var query = request.ToQuery();
            var result = await mediator.Send(query, context.CancellationToken);
            return result.ToResponse();
        }
    }
}
