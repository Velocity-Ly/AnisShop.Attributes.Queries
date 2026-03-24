using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Requests
{
    public class GetRequestFaker : Faker<GetRequest>
    {
        public GetRequestFaker()
        {
            CustomInstantiator(f => new GetRequest
            {
                Id = f.Random.Guid().ToString(),
            });
        }

        public GetRequestFaker WithId(Guid id)
        {
            CustomInstantiator(f => new GetRequest
            {
                Id = id.ToString(),
            });
            return this;
        }
    }
}
