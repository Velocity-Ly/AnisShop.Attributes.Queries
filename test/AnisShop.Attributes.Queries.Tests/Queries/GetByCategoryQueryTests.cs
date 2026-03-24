using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.Helpers;
using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Grpc.Core;
using Microsoft.AspNetCore.Mvc.Testing;
using System.Text.Json;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.Queries
{
    public class GetByCategoryQueryTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly GrpcClientHelper _grpcClientHelper;
        private readonly DatabaseHelper _databaseHelper;

        public GetByCategoryQueryTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _grpcClientHelper = new GrpcClientHelper(_factory);
            _databaseHelper = new DatabaseHelper(_factory);
        }

        [Fact]
        public async Task GetByCategory_WithMatchingAttributes_ReturnSuccess()
        {
            // Arrange
            const int categoryId = 100;
            var attributes = new[]
            {
                new AttributeFaker().WithCategoryIds(categoryId).Generate(),
                new AttributeFaker().WithCategoryIds(categoryId).Generate(),
            };
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByCategoryRequest
            {
                CategoryId = categoryId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Equal(1, response.CurrentPage);
            Assert.Equal(10, response.PageSize);
            AssertEquality.OfDomainAndQueryResponse(attributes, response);
        }

        [Fact]
        public async Task GetByCategory_NoMatchingAttributes_ReturnEmptyList()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithCategoryIds(200));

            var request = new GetByCategoryRequest
            {
                CategoryId = 999,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Empty(response.Attributes);
        }

        [Fact]
        public async Task GetByCategory_WithUnrelatedAttributes_OnlyReturnMatching()
        {
            // Arrange
            const int targetCategoryId = 300;
            var matchingAttribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithCategoryIds(targetCategoryId));
            var unrelatedAttribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithCategoryIds(301));

            var request = new GetByCategoryRequest
            {
                CategoryId = targetCategoryId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Single(response.Attributes);
            Assert.Equal(matchingAttribute.Id.ToString(), response.Attributes[0].Id);
        }

        [Theory]
        [InlineData(1, 10)]
        [InlineData(1, 5)]
        [InlineData(2, 5)]
        [InlineData(1, 1)]
        public async Task GetByCategory_WithPagination_ReturnCorrectPage(int currentPage, int pageSize)
        {
            // Arrange
            const int categoryId = 400;
            var attributes = Enumerable.Range(0, 10)
                .Select(_ => new AttributeFaker().WithCategoryIds(categoryId).Generate())
                .ToArray();
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByCategoryRequest
            {
                CategoryId = categoryId,
                CurrentPage = currentPage,
                PageSize = pageSize,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Equal(currentPage, response.CurrentPage);
            Assert.Equal(pageSize, response.PageSize);

            var expectedCount = Math.Min(pageSize, 10 - ((currentPage - 1) * pageSize));
            Assert.Equal(expectedCount, response.Attributes.Count);
        }

        [Fact]
        public async Task GetByCategory_PageBeyondTotal_ReturnEmptyList()
        {
            // Arrange
            const int categoryId = 500;
            var attributes = new[]
            {
                new AttributeFaker().WithCategoryIds(categoryId).Generate(),
                new AttributeFaker().WithCategoryIds(categoryId).Generate(),
            };
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByCategoryRequest
            {
                CategoryId = categoryId,
                CurrentPage = 10,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Empty(response.Attributes);
            Assert.Equal(10, response.CurrentPage);
            Assert.Equal(10, response.PageSize);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public async Task GetByCategory_WithInvalidCategoryId_ThrowsInvalidArgument(int categoryId)
        {
            // Arrange
            var request = new GetByCategoryRequest
            {
                CategoryId = categoryId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request)));

            Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);

            var errorsJson = exception.Trailers.Single(x => x.Key == "errors").Value;
            var errors = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(errorsJson);

            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.True(errors.ContainsKey(nameof(GetByCategoryRequest.CategoryId)));
        }

        [Theory]
        [InlineData(0, 10)]
        [InlineData(-1, 10)]
        [InlineData(1, 0)]
        [InlineData(1, -1)]
        public async Task GetByCategory_WithInvalidPagination_ThrowsInvalidArgument(int currentPage, int pageSize)
        {
            // Arrange
            var request = new GetByCategoryRequest
            {
                CategoryId = 1,
                CurrentPage = currentPage,
                PageSize = pageSize,
            };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request)));

            Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);

            var errorsJson = exception.Trailers.Single(x => x.Key == "errors").Value;
            var errors = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(errorsJson);

            Assert.NotNull(errors);
            Assert.Single(errors);
        }

        [Fact]
        public async Task GetByCategory_AttributeWithMultipleCategories_ReturnWhenMatchingAny()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithCategoryIds(600, 601, 602));

            var request = new GetByCategoryRequest
            {
                CategoryId = 601,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Single(response.Attributes);
            Assert.Equal(attribute.Id.ToString(), response.Attributes[0].Id);
        }

        [Fact]
        public async Task GetByCategory_VerifyPaginationTotal_MatchesActualCount()
        {
            // Arrange
            const int categoryId = 700;
            var attributes = Enumerable.Range(0, 15)
                .Select(_ => new AttributeFaker().WithCategoryIds(categoryId).Generate())
                .ToArray();
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByCategoryRequest
            {
                CategoryId = categoryId,
                CurrentPage = 1,
                PageSize = 5,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByCategoryAsync(request));

            // Assert
            Assert.Equal(5, response.Attributes.Count);
            Assert.Equal(1, response.CurrentPage);
            Assert.Equal(5, response.PageSize);
        }
    }
}
