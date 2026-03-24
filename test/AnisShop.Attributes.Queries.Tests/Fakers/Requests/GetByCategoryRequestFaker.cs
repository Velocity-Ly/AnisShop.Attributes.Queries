using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Bogus;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Requests
{
    public class GetByCategoryRequestFaker : Faker<GetByCategoryRequest>
    {
        public GetByCategoryRequestFaker()
        {
            CustomInstantiator(f => new GetByCategoryRequest
            {
                CategoryId = f.Random.Int(1, 1000),
                CurrentPage = 1,
                PageSize = 10,
            });
        }

        public GetByCategoryRequestFaker WithCategoryId(int categoryId)
        {
            CustomInstantiator(f => new GetByCategoryRequest
            {
                CategoryId = categoryId,
                CurrentPage = 1,
                PageSize = 10,
            });
            return this;
        }

        public GetByCategoryRequestFaker WithPagination(int currentPage, int pageSize)
        {
            CustomInstantiator(f => new GetByCategoryRequest
            {
                CategoryId = f.Random.Int(1, 1000),
                CurrentPage = currentPage,
                PageSize = pageSize,
            });
            return this;
        }
    }
}
