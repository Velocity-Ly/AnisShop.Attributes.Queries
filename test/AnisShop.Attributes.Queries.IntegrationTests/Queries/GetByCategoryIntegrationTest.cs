using AnisShop.Attributes.Queries.IntegrationTests.Fixtures;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.IntegrationTests.Queries;

public class GetByCategoryIntegrationTest(LocalDbFixture fixture, ITestOutputHelper output)
    : SqlIntegrationTestBase(fixture, output)
{
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
        await DatabaseHelper.InsertAsync(attributes);

        var request = new GetByCategoryRequest
        {
            CategoryId = categoryId,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByCategoryAsync(request));

        // Assert
        Assert.Equal(1, response.CurrentPage);
        Assert.Equal(10, response.PageSize);
        AssertEquality.OfDomainAndQueryResponse(attributes, response);
    }

    [Fact]
    public async Task GetByCategory_NoMatchingAttributes_ReturnEmptyList()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithCategoryIds(200));

        var request = new GetByCategoryRequest
        {
            CategoryId = 999,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByCategoryAsync(request));

        // Assert
        Assert.Empty(response.Attributes);
    }

    [Fact]
    public async Task GetByCategory_WithUnrelatedAttributes_OnlyReturnMatching()
    {
        // Arrange
        const int targetCategoryId = 300;
        var matchingAttribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithCategoryIds(targetCategoryId));
        var unrelatedAttribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithCategoryIds(301));

        var request = new GetByCategoryRequest
        {
            CategoryId = targetCategoryId,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByCategoryAsync(request));

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
        await DatabaseHelper.InsertAsync(attributes);

        var request = new GetByCategoryRequest
        {
            CategoryId = categoryId,
            CurrentPage = currentPage,
            PageSize = pageSize,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByCategoryAsync(request));

        // Assert
        Assert.Equal(currentPage, response.CurrentPage);
        Assert.Equal(pageSize, response.PageSize);

        var expectedCount = Math.Min(pageSize, 10 - ((currentPage - 1) * pageSize));
        Assert.Equal(expectedCount, response.Attributes.Count);
    }

    [Fact]
    public async Task GetByCategory_AttributeWithMultipleCategories_ReturnWhenMatchingAny()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithCategoryIds(600, 601, 602));

        var request = new GetByCategoryRequest
        {
            CategoryId = 601,
            CurrentPage = 1,
            PageSize = 10,
        };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetByCategoryAsync(request));

        // Assert
        Assert.Single(response.Attributes);
        Assert.Equal(attribute.Id.ToString(), response.Attributes[0].Id);
    }
}
