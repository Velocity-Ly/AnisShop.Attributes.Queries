using AnisShop.Attributes.Queries.GrpcServices;
using AnisShop.Attributes.Queries.Infrastructure.Messaging;
using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.Interceptors;
using AnisShop.Attributes.Queries.Setup;
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

// Service Bus, Kafka or KafkaFlow, selected by "Messaging:Transport". All three project through
// IncomingEvents.
builder.Services.AddEventListener(builder.Configuration);

builder.Host.UseSerilog();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<AttributesQueriesService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
