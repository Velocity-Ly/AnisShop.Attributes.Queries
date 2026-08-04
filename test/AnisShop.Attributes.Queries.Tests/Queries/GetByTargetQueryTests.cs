using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.Helpers;
using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Grpc.Core;
using Microsoft.AspNetCore.Mvc.Testing;
using System.Text.Json;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Queries
{
    public class GetByTargetQueryTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly GrpcClientHelper _grpcClientHelper;
        private readonly DatabaseHelper _databaseHelper;

        public GetByTargetQueryTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _grpcClientHelper = new GrpcClientHelper(_factory);
            _databaseHelper = new DatabaseHelper(_factory);
        }

        [Fact]
        public async Task GetByTarget_WithMatchingAttributes_ReturnSuccess()
        {
            // Arrange
            const int targetId = 100;
            var attributes = new[]
            {
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId).Generate(),
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId).Generate(),
            };
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Equal(1, response.CurrentPage);
            Assert.Equal(10, response.PageSize);
            AssertEquality.OfDomainAndQueryResponse(attributes, response);
        }

        [Fact]
        public async Task GetByTarget_NoMatchingAttributes_ReturnEmptyList()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(200));

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = 999,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Empty(response.Attributes);
        }

        [Fact]
        public async Task GetByTarget_WithUnrelatedAttributes_OnlyReturnMatching()
        {
            // Arrange
            const int matchingTargetId = 300;
            var matchingAttribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(matchingTargetId));
            var unrelatedAttribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(301));

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = matchingTargetId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Single(response.Attributes);
            Assert.Equal(matchingAttribute.Id.ToString(), response.Attributes[0].Id);
        }

        [Fact]
        public async Task GetByTarget_SameTargetIdDifferentScope_ExcludesMismatchedScope()
        {
            // Arrange: two attributes share the same target id but differ in scope. Target ids
            // are only meaningful within a scope, so a ProductCategory query must NOT return the
            // MarketType attribute even though their numeric target ids collide.
            const int targetId = 555;
            var productCategoryAttribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId));
            var marketTypeAttribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.MarketType).WithTargetIds(targetId));

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert: only the ProductCategory-scoped attribute matches
            Assert.Single(response.Attributes);
            Assert.Equal(productCategoryAttribute.Id.ToString(), response.Attributes[0].Id);
        }

        [Theory]
        [InlineData(1, 10)]
        [InlineData(1, 5)]
        [InlineData(2, 5)]
        [InlineData(1, 1)]
        public async Task GetByTarget_WithPagination_ReturnCorrectPage(int currentPage, int pageSize)
        {
            // Arrange
            const int targetId = 400;
            var attributes = Enumerable.Range(0, 10)
                .Select(_ => new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId).Generate())
                .ToArray();
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = currentPage,
                PageSize = pageSize,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Equal(currentPage, response.CurrentPage);
            Assert.Equal(pageSize, response.PageSize);

            var expectedCount = Math.Min(pageSize, 10 - ((currentPage - 1) * pageSize));
            Assert.Equal(expectedCount, response.Attributes.Count);
        }

        [Fact]
        public async Task GetByTarget_PageBeyondTotal_ReturnEmptyList()
        {
            // Arrange
            const int targetId = 500;
            var attributes = new[]
            {
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId).Generate(),
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId).Generate(),
            };
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = 10,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Empty(response.Attributes);
            Assert.Equal(10, response.CurrentPage);
            Assert.Equal(10, response.PageSize);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public async Task GetByTarget_WithInvalidTargetId_ThrowsInvalidArgument(int targetId)
        {
            // Arrange
            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetByTargetAsync(request)));

            Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);

            var errorsJson = exception.Trailers.Single(x => x.Key == "errors").Value;
            var errors = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(errorsJson);

            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.True(errors.ContainsKey(nameof(GetByTargetRequest.TargetId)));
        }

        [Fact]
        public async Task GetByTarget_WithUnspecifiedScope_ThrowsInvalidArgument()
        {
            // Arrange
            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.Unspecified,
                TargetId = 1,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetByTargetAsync(request)));

            Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);

            var errorsJson = exception.Trailers.Single(x => x.Key == "errors").Value;
            var errors = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(errorsJson);

            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.True(errors.ContainsKey(nameof(GetByTargetRequest.Scope)));
        }

        [Theory]
        [InlineData(0, 10)]
        [InlineData(-1, 10)]
        [InlineData(1, 0)]
        [InlineData(1, -1)]
        public async Task GetByTarget_WithInvalidPagination_ThrowsInvalidArgument(int currentPage, int pageSize)
        {
            // Arrange
            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = 1,
                CurrentPage = currentPage,
                PageSize = pageSize,
            };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetByTargetAsync(request)));

            Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);

            var errorsJson = exception.Trailers.Single(x => x.Key == "errors").Value;
            var errors = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(errorsJson);

            Assert.NotNull(errors);
            Assert.Single(errors);
        }

        [Fact]
        public async Task GetByTarget_AttributeWithMultipleTargets_ReturnWhenMatchingAny()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(600, 601, 602));

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = 601,
                CurrentPage = 1,
                PageSize = 10,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Single(response.Attributes);
            Assert.Equal(attribute.Id.ToString(), response.Attributes[0].Id);
        }

        [Fact]
        public async Task GetByTarget_VerifyPaginationTotal_MatchesActualCount()
        {
            // Arrange
            const int targetId = 700;
            var attributes = Enumerable.Range(0, 15)
                .Select(_ => new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId).Generate())
                .ToArray();
            await _databaseHelper.InsertAsync(attributes);

            var request = new GetByTargetRequest
            {
                Scope = AttributeScope.ProductCategory,
                TargetId = targetId,
                CurrentPage = 1,
                PageSize = 5,
            };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetByTargetAsync(request));

            // Assert
            Assert.Equal(5, response.Attributes.Count);
            Assert.Equal(1, response.CurrentPage);
            Assert.Equal(5, response.PageSize);
        }
    }
}
