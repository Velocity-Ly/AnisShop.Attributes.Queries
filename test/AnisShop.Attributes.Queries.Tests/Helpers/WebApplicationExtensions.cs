using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Xunit.Abstractions;

namespace AnisShop.Attributes.Queries.Tests.Helpers
{
    public static class WebApplicationExtensions
    {
        public static WebApplicationFactory<Program> WithDefaultConfigurations(
            this WebApplicationFactory<Program> factory,
            ITestOutputHelper helper,
            Action<IServiceCollection>? servicesConfiguration = null) => factory
               .WithWebHostBuilder(builder =>
               {
                   builder.ConfigureLogging(loggingBuilder =>
                   {
                       Log.Logger = new LoggerConfiguration()
                           .MinimumLevel.Information()
                           .WriteTo.TestOutput(helper, LogEventLevel.Information)
                           .CreateLogger();
                   });

                   if (servicesConfiguration != null)
                       builder.ConfigureTestServices(servicesConfiguration);
               });

        public static GrpcChannel CreateGrpcChannel(this WebApplicationFactory<Program> factory)
        {
            var client = factory.CreateDefaultClient();

            return GrpcChannel.ForAddress(client.BaseAddress ?? throw new InvalidOperationException("BaseAddress is null"), new GrpcChannelOptions
            {
                HttpClient = client,
            });
        }
    }
}
