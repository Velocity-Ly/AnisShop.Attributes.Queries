// A reference KafkaFlow producer for the query side's event contract.
//
// It publishes a handful of aggregates, each as a short, in-order version stream, exactly the way a
// command service should so the KafkaFlow listener's guarantees hold. The three things that matter
// are all in Publish() at the bottom: the key, the type header, and the camelCase JSON body.
//
// Run it against the same topic the listener consumes and watch them project.
//
//   KAFKA_BOOTSTRAP=host:9092,host:9092  KAFKA_TOPIC=app-events \
//   KAFKA_USERNAME=app  KAFKA_PASSWORD=****  dotnet run -- 5      # 5 aggregates

using System.Text;
using System.Text.Json;
using AnisShop.Attributes.Sample.Publisher;
using KafkaFlow;
using KafkaFlow.Producers;
using Microsoft.Extensions.DependencyInjection;

const string producerName = "attributes-events";
const string typeHeader = "type";

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";
var topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "app-events";
var username = Environment.GetEnvironmentVariable("KAFKA_USERNAME");
var password = Environment.GetEnvironmentVariable("KAFKA_PASSWORD");
var aggregates = args.Length > 0 && int.TryParse(args[0], out var parsed) ? parsed : 3;

// The one contract that must match the consumer: property names are camelCase on the wire.
var json = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

var services = new ServiceCollection();

services.AddKafka(kafka => kafka
    .AddCluster(cluster =>
    {
        cluster.WithBrokers(bootstrap.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));

        // Same posture as the listener. Inside a VPN, SASL_PLAINTEXT + SCRAM-SHA-512; over an
        // untrusted network use SaslSsl. Credentials come from the environment, never source.
        if (!string.IsNullOrEmpty(username))
        {
            cluster.WithSecurityInformation(security =>
            {
                security.SecurityProtocol = KafkaFlow.Configuration.SecurityProtocol.SaslPlaintext;
                security.SaslMechanism = KafkaFlow.Configuration.SaslMechanism.ScramSha512;
                security.SaslUsername = username;
                security.SaslPassword = password;
            });
        }

        cluster.AddProducer(producerName, producer => producer
            .DefaultTopic(topic)
            // acks=all: a write is acknowledged only once the in-sync replicas have it — no silent loss.
            .WithAcks(KafkaFlow.Acks.All)
            // Idempotent producer: retries cannot duplicate a message or reorder a key. This is what
            // makes at-least-once safe to pair with the consumer's version-idempotent projection.
            .WithProducerConfig(new Confluent.Kafka.ProducerConfig
            {
                EnableIdempotence = true,
            }));
    }));

using var provider = services.BuildServiceProvider();
var bus = provider.CreateKafkaBus();
await bus.StartAsync();

var producer = provider.GetRequiredService<IProducerAccessor>().GetProducer(producerName);

Console.WriteLine($"publishing {aggregates} aggregate(s) x 3 events -> {topic} @ {bootstrap}");

for (var i = 0; i < aggregates; i++)
{
    var id = Guid.NewGuid();
    var now = DateTime.UtcNow;

    // ONE aggregate, its events produced in version order. Same key -> same partition -> the consumer
    // receives them in exactly this order. Never start a stream at anything but v1 with a Created.
    await Publish(id, new AttributeCreated
    {
        AggregateId = id,
        Version = 1,
        UserId = "sample",
        DateTime = now,
        Data = new AttributeCreated.CreatedData
        {
            Metadata = new MetadataData
            {
                ArabicDisplayName = $"سمة {i}",
                EnglishDisplayName = $"Attribute {i}",
                EnglishDescription = "published by the KafkaFlow sample",
            },
            Type = i % 2 == 0 ? "SingleSelect" : "MultiSelect",
        },
    });

    await Publish(id, new AttributePublished
    {
        AggregateId = id,
        Version = 2,
        UserId = "sample",
        DateTime = now,
    });

    await Publish(id, new AttributeMetadataChanged
    {
        AggregateId = id,
        Version = 3,
        UserId = "sample",
        DateTime = now,
        Data = new AttributeMetadataChanged.MetadataChangedData
        {
            Metadata = new MetadataData
            {
                ArabicDisplayName = $"سمة {i} — محدثة",
                EnglishDisplayName = $"Attribute {i} (updated)",
            },
        },
    });

    Console.WriteLine($"  {id}  ->  v1 Created, v2 Published, v3 MetadataChanged");
}

await bus.StopAsync();
Console.WriteLine("done.");

// Everything that makes a message consumable is here.
async Task Publish(Guid aggregateId, EventBase @event)
{
    // 1. KEY = AggregateId, as a UTF-8 string. This is the whole ordering guarantee: same key ->
    //    same partition -> same consumer worker -> one aggregate, strictly ordered.
    var key = Encoding.UTF8.GetBytes(aggregateId.ToString());

    // 2. TYPE HEADER. The consumer has no per-message schema; it reads this header to know which
    //    event type to deserialize into. It must be the event type's name.
    var headers = new MessageHeaders();
    headers.Add(typeHeader, Encoding.UTF8.GetBytes(@event.GetType().Name));

    // 3. VALUE = the event as camelCase JSON, matching the consumer's serializer options exactly.
    var value = JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), json);

    await producer.ProduceAsync(topic, key, value, headers);
}
