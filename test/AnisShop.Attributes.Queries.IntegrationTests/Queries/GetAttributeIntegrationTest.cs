using AnisShop.Attributes.Queries.IntegrationTests.Fixtures;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.IntegrationTests.Queries;

public class GetAttributeIntegrationTest(LocalDbFixture fixture, ITestOutputHelper output)
    : SqlIntegrationTestBase(fixture, output)
{
    [Fact]
    public async Task Get_ExistingAttribute_ReturnSuccess()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(new AttributeFaker());
        var request = new GetRequest { Id = attribute.Id.ToString() };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetAsync(request));

        // Assert
        AssertEquality.OfDomainAndResponse(attribute, response);
    }

    [Fact]
    public async Task Get_AttributeWithOptions_ReturnSuccessWithOptions()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithOptions(3));
        var request = new GetRequest { Id = attribute.Id.ToString() };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetAsync(request));

        // Assert
        AssertEquality.OfDomainAndResponse(attribute, response);
        Assert.Equal(3, response.Attribute.Options.Count);
    }

    [Fact]
    public async Task Get_AttributeWithTargets_ReturnSuccessWithTargets()
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithTargetIds(1, 2, 3));
        var request = new GetRequest { Id = attribute.Id.ToString() };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetAsync(request));

        // Assert
        AssertEquality.OfDomainAndResponse(attribute, response);
        Assert.Equal(3, response.Attribute.ApplicableTargetIds.Count);
    }

    [Theory]
    [InlineData(SourceDomain.AttributeStatus.Draft)]
    [InlineData(SourceDomain.AttributeStatus.Published)]
    [InlineData(SourceDomain.AttributeStatus.Deprecated)]
    [InlineData(SourceDomain.AttributeStatus.Disabled)]
    public async Task Get_AttributeWithDifferentStatuses_ReturnCorrectStatus(SourceDomain.AttributeStatus status)
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithStatus(status));
        var request = new GetRequest { Id = attribute.Id.ToString() };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetAsync(request));

        // Assert
        AssertEquality.OfDomainAndResponse(attribute, response);
    }

    [Theory]
    [InlineData(SourceDomain.AttributeType.SingleSelect)]
    [InlineData(SourceDomain.AttributeType.MultiSelect)]
    public async Task Get_AttributeWithDifferentTypes_ReturnCorrectType(SourceDomain.AttributeType type)
    {
        // Arrange
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithType(type));
        var request = new GetRequest { Id = attribute.Id.ToString() };

        // Act
        var response = await GrpcClientHelper.Query(x => x.GetAsync(request));

        // Assert
        AssertEquality.OfDomainAndResponse(attribute, response);
    }
}
