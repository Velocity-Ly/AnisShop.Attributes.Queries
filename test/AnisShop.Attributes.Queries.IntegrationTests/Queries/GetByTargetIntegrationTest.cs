using AnisShop.Attributes.Queries.IntegrationTests.Fixtures;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.IntegrationTests.Queries;

public class GetByTargetIntegrationTest(LocalDbFixture fixture, ITestOutputHelper output)
    : SqlIntegrationTestBase(fixture, output)
{
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
        await DatabaseHelper.InsertAsync(attributes);

        var request = new GetByTargetRequest
        {
            Scope = AttributeScope.ProductCategory,
            TargetId = targetId,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByTargetAsync(request));

        // Assert
        Assert.Equal(1, response.CurrentPage);
        Assert.Equal(10, response.PageSize);
        AssertEquality.OfDomainAndQueryResponse(attributes, response);
    }

    [Fact]
    public async Task GetByTarget_NoMatchingAttributes_ReturnEmptyList()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(200));

        var request = new GetByTargetRequest
        {
            Scope = AttributeScope.ProductCategory,
            TargetId = 999,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByTargetAsync(request));

        // Assert
        Assert.Empty(response.Attributes);
    }

    [Fact]
    public async Task GetByTarget_WithUnrelatedAttributes_OnlyReturnMatching()
    {
        // Arrange
        const int matchingTargetId = 300;
        var matchingAttribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(matchingTargetId));
        var unrelatedAttribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(301));

        var request = new GetByTargetRequest
        {
            Scope = AttributeScope.ProductCategory,
            TargetId = matchingTargetId,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByTargetAsync(request));

        // Assert
        Assert.Single(response.Attributes);
        Assert.Equal(matchingAttribute.Id.ToString(), response.Attributes[0].Id);
    }

    [Fact]
    public async Task GetByTarget_SameTargetIdDifferentScope_ExcludesMismatchedScope()
    {
        // Arrange: same numeric target id, different scope. The scope filter must isolate them.
        const int targetId = 555;
        var productCategoryAttribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(targetId));
        var marketTypeAttribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithScope(SourceDomain.AttributeScope.MarketType).WithTargetIds(targetId));

        var request = new GetByTargetRequest
        {
            Scope = AttributeScope.MarketType,
            TargetId = targetId,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByTargetAsync(request));

        // Assert: only the MarketType-scoped attribute matches
        Assert.Single(response.Attributes);
        Assert.Equal(marketTypeAttribute.Id.ToString(), response.Attributes[0].Id);
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
        await DatabaseHelper.InsertAsync(attributes);

        var request = new GetByTargetRequest
        {
            Scope = AttributeScope.ProductCategory,
            TargetId = targetId,
            CurrentPage = currentPage,
            PageSize = pageSize,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByTargetAsync(request));

        // Assert
        Assert.Equal(currentPage, response.CurrentPage);
        Assert.Equal(pageSize, response.PageSize);

        var expectedCount = Math.Min(pageSize, 10 - ((currentPage - 1) * pageSize));
        Assert.Equal(expectedCount, response.Attributes.Count);
    }

    [Fact]
    public async Task GetByTarget_AttributeWithMultipleTargets_ReturnWhenMatchingAny()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithScope(SourceDomain.AttributeScope.ProductCategory).WithTargetIds(600, 601, 602));

        var request = new GetByTargetRequest
        {
            Scope = AttributeScope.ProductCategory,
            TargetId = 601,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByTargetAsync(request));

        // Assert
        Assert.Single(response.Attributes);
        Assert.Equal(attribute.Id.ToString(), response.Attributes[0].Id);
    }
}
