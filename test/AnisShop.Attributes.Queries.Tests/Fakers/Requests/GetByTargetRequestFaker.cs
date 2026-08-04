using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Requests
{
    public class GetByTargetRequestFaker : Faker<GetByTargetRequest>
    {
        public GetByTargetRequestFaker()
        {
            CustomInstantiator(f => new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = f.Random.Int(1, 1000),
                CurrentPage = 1,
                PageSize = 10,
            });
        }

        public GetByTargetRequestFaker WithScope(AttributeScope scope)
        {
            CustomInstantiator(f => new GetByTargetRequest
            {
                Scope = scope,
                TargetId = f.Random.Int(1, 1000),
                CurrentPage = 1,
                PageSize = 10,
            });
            return this;
        }

        public GetByTargetRequestFaker WithTargetId(int targetId)
        {
            CustomInstantiator(f => new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = 1,
                PageSize = 10,
            });
            return this;
        }

        public GetByTargetRequestFaker WithPagination(int currentPage, int pageSize)
        {
            CustomInstantiator(f => new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = f.Random.Int(1, 1000),
                CurrentPage = currentPage,
                PageSize = pageSize,
            });
            return this;
        }
    }
}
