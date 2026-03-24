using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Respawn;

namespace AnisShop.Attributes.Queries.IntegrationTests.Fixtures;

public class LocalDbFixture : IAsyncLifetime
{
    private const string DefaultConnectionString =
        "Server=(localdb)\\MSSQLLocalDB;Database=AttributesQueriesIntegrationTests;Trusted_Connection=True;TrustServerCertificate=True;";

    private Respawner? _respawner;

    public string ConnectionString { get; } =
        Environment.GetEnvironmentVariable("TEST_DB_CONNECTION_STRING") ?? DefaultConnectionString;

    public async Task InitializeAsync()
    {
        var options = new DbContextOptionsBuilder<AttributesDbContext>()
            .UseSqlServer(ConnectionString)
            .Options;

        await using var context = new AttributesDbContext(options);
        await context.Database.EnsureCreatedAsync();

        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();

        _respawner = await Respawner.CreateAsync(connection, new RespawnerOptions
        {
            DbAdapter = DbAdapter.SqlServer,
            SchemasToInclude = ["dbo"]
        });
    }

    public async Task ResetDatabaseAsync()
    {
        if (_respawner is null)
            throw new InvalidOperationException("Fixture not initialized. Call InitializeAsync first.");

        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await _respawner.ResetAsync(connection);
    }

    public Task DisposeAsync() => Task.CompletedTask;
}
