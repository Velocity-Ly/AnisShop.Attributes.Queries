using AnisShop.Attributes.Queries.GrpcServices;
using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.Infrastructure.ServiceBus;
using AnisShop.Attributes.Queries.Interceptors;
using AnisShop.Attributes.Queries.Setup;
using Azure.Messaging.ServiceBus;
using FluentValidation;
using Microsoft.EntityFrameworkCore;
using Serilog;

Log.Logger = LoggerServiceBuilder.Build();

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<CultureInterceptor>();
    options.Interceptors.Add<ValidationInterceptor>();
});

builder.Services.AddValidatorsFromAssemblyContaining<Program>(ServiceLifetime.Transient);

builder.Services.AddMediator(o => o.ServiceLifetime = ServiceLifetime.Transient);

builder.Services.AddDbContext<AttributesDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("AttributesDatabase")));

builder.Services.AddHostedService<DatabaseRunner>();

builder.Services.AddSingleton(_ =>
    new ServiceBusClient(builder.Configuration.GetConnectionString("ServiceBus")));

builder.Services.Configure<ServiceBusListenerOptions>(
    builder.Configuration.GetSection(ServiceBusListenerOptions.SectionName));

builder.Services.AddSingleton<IEventDeserializer, EventDeserializer>();
builder.Services.AddSingleton<EventBatchProcessor>();
builder.Services.AddHostedService<ServiceBusEventListener>();

builder.Host.UseSerilog();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<AttributesQueriesService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
