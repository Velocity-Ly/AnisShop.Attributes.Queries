namespace AnisShop.Attributes.Queries.IntegrationTests.Fixtures;

[CollectionDefinition(Name)]
public class IntegrationTestCollection : ICollectionFixture<LocalDbFixture>
{
    public const string Name = "SqlIntegration";
}
