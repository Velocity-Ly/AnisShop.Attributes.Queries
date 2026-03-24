using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Microsoft.AspNetCore.Mvc.Testing;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public class GrpcClientHelper(WebApplicationFactory<Program> factory)
    {
        public TResult Query<TResult>(Func<AttributesQueries.AttributesQueriesClient, TResult> query)
        {
            var client = new AttributesQueries.AttributesQueriesClient(factory.CreateGrpcChannel());
            return query(client);
        }
    }
}
