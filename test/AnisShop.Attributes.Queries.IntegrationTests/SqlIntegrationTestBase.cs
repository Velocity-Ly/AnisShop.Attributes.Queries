using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.IntegrationTests.Fixtures;
using AnisShop.Attributes.Queries.Tests.Helpers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Events;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.IntegrationTests;

[Collection(IntegrationTestCollection.Name)]
public abstract class SqlIntegrationTestBase : IAsyncLifetime
{
    private readonly LocalDbFixture _fixture;
    protected readonly WebApplicationFactory<Program> Factory;
    protected readonly GrpcClientHelper GrpcClientHelper;
    protected readonly DatabaseHelper DatabaseHelper;
    protected readonly MediatorHelper MediatorHelper;

    protected SqlIntegrationTestBase(LocalDbFixture fixture, ITestOutputHelper output, Action<IServiceCollection>? additionalConfiguration = null)
    {
        _fixture = fixture;

        Factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureLogging(loggingBuilder =>
                {
                    Log.Logger = new LoggerConfiguration()
                        .MinimumLevel.Information()
                        .WriteTo.TestOutput(output, LogEventLevel.Information)
                        .CreateLogger();
                });

                builder.ConfigureTestServices(services =>
                {
                    ConfigureSqlServerEnvironment(services, fixture.ConnectionString);
                    additionalConfiguration?.Invoke(services);
                });
            });

        GrpcClientHelper = new GrpcClientHelper(Factory);
        DatabaseHelper = new DatabaseHelper(Factory);
        MediatorHelper = new MediatorHelper(Factory);
    }

    public Task InitializeAsync() => _fixture.ResetDatabaseAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    private static void ConfigureSqlServerEnvironment(IServiceCollection services, string connectionString)
    {
        RemoveDatabaseRunner(services);
        services.RemoveEventListeners();
        UseSqlServer(services, connectionString);
    }

    private static void RemoveDatabaseRunner(IServiceCollection services)
    {
        var descriptor = services.Single(d => d.ImplementationType == typeof(DatabaseRunner));
        services.Remove(descriptor);
    }

    private static void UseSqlServer(IServiceCollection services, string connectionString)
    {
        var optionsConfig = services
            .Where(r => r.ServiceType.IsGenericType &&
                        r.ServiceType.GetGenericTypeDefinition() == typeof(IDbContextOptionsConfiguration<>))
            .Single();
        services.Remove(optionsConfig);

        services.AddDbContext<AttributesDbContext>(options => options.UseSqlServer(connectionString));
    }
}
