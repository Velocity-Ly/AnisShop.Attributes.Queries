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
    public class GetAttributeQueryTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly GrpcClientHelper _grpcClientHelper;
        private readonly DatabaseHelper _databaseHelper;

        public GetAttributeQueryTests(WebApplicationFactory<Program> factory, ITestOutputHelper helper)
        {
            _factory = factory.WithDefaultConfigurations(helper, services =>
            {
                services.SetDefaultUnitTestsEnvironment();
            });

            _grpcClientHelper = new GrpcClientHelper(_factory);
            _databaseHelper = new DatabaseHelper(_factory);
        }

        [Fact]
        public async Task Get_ExistingAttribute_ReturnSuccess()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(new AttributeFaker());
            var request = new GetRequest { Id = attribute.Id.ToString() };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetAsync(request));

            // Assert
            AssertEquality.OfDomainAndResponse(attribute, response);
        }

        [Fact]
        public async Task Get_AttributeWithOptions_ReturnSuccessWithOptions()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithOptions(3));
            var request = new GetRequest { Id = attribute.Id.ToString() };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetAsync(request));

            // Assert
            AssertEquality.OfDomainAndResponse(attribute, response);
            Assert.Equal(3, response.Attribute.Options.Count);
        }

        [Fact]
        public async Task Get_AttributeWithTargets_ReturnSuccessWithTargets()
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithTargetIds(1, 2, 3));
            var request = new GetRequest { Id = attribute.Id.ToString() };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetAsync(request));

            // Assert
            AssertEquality.OfDomainAndResponse(attribute, response);
            Assert.Equal(3, response.Attribute.ApplicableTargetIds.Count);
            Assert.Contains(1, response.Attribute.ApplicableTargetIds);
            Assert.Contains(2, response.Attribute.ApplicableTargetIds);
            Assert.Contains(3, response.Attribute.ApplicableTargetIds);
        }

        [Theory]
        [InlineData("")]
        [InlineData("invalid-guid")]
        [InlineData("12345")]
        public async Task Get_WithInvalidId_ThrowsInvalidArgument(string id)
        {
            // Arrange
            var request = new GetRequest { Id = id };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetAsync(request)));

            Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);

            var errorsJson = exception.Trailers.Single(x => x.Key == "errors").Value;
            var errors = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(errorsJson);

            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.True(errors.ContainsKey(nameof(GetRequest.Id)));
        }

        [Fact]
        public async Task Get_WithNonExistingId_ThrowsNotFound()
        {
            // Arrange
            var request = new GetRequest { Id = Guid.NewGuid().ToString() };

            // Act & Assert
            var exception = await Assert.ThrowsAsync<RpcException>(
                async () => await _grpcClientHelper.Query(x => x.GetAsync(request)));

            Assert.Equal(StatusCode.NotFound, exception.StatusCode);
        }

        [Theory]
        [InlineData(Domain.AttributeStatus.Draft)]
        [InlineData(Domain.AttributeStatus.Published)]
        [InlineData(Domain.AttributeStatus.Deprecated)]
        [InlineData(Domain.AttributeStatus.Disabled)]
        public async Task Get_AttributeWithDifferentStatuses_ReturnCorrectStatus(Domain.AttributeStatus status)
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithStatus(status));
            var request = new GetRequest { Id = attribute.Id.ToString() };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetAsync(request));

            // Assert
            AssertEquality.OfDomainAndResponse(attribute, response);
        }

        [Theory]
        [InlineData(Domain.AttributeType.SingleSelect)]
        [InlineData(Domain.AttributeType.MultiSelect)]
        public async Task Get_AttributeWithDifferentTypes_ReturnCorrectType(Domain.AttributeType type)
        {
            // Arrange
            var attribute = await _databaseHelper.InsertAsync(
                new AttributeFaker().WithType(type));
            var request = new GetRequest { Id = attribute.Id.ToString() };

            // Act
            var response = await _grpcClientHelper.Query(x => x.GetAsync(request));

            // Assert
            AssertEquality.OfDomainAndResponse(attribute, response);
        }
    }
}
