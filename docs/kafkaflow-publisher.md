# KafkaFlow — publisher configuration & best practices

> **New to KafkaFlow here?** Start at [kafkaflow.md](kafkaflow.md) — the single entry point.
>
> **Purpose**: how a command service should **publish** events with KafkaFlow so the query side's
> ordering and projection guarantees hold. The consumer half is
> [`kafkaflow-best-practices.md`](kafkaflow-best-practices.md); this is the half that produces what it
> reads, and the guarantees actually start here.
>
> A working, verified producer lives in
> [`samples/AnisShop.Attributes.Sample.Publisher`](../samples/AnisShop.Attributes.Sample.Publisher) —
> it published against the real cluster and the listener projected every message. Read it alongside
> this.

---

## The contract, in three parts

The consumer has no per-message schema negotiation. It reads three things off every message, and if
any one is wrong the message is a **poison message**. Every one of them is the publisher's job:

| Part | What it must be | Consumer reads it as |
|---|---|---|
| **Key** | `AggregateId` as a UTF-8 string | the ordering identity — same key → same partition → one ordered stream |
| **`type` header** | the event type's name, e.g. `AttributeCreated` | the type to deserialize into |
| **Value** | the event as **camelCase** JSON | the event body |

Get the key wrong and ordering is gone. Get the header or JSON wrong and the message cannot be
deserialized — which, on a dedicated topic, the consumer treats as fatal.

The producer here writes **raw bytes with an explicit header** and no serializer middleware —
deliberately symmetric to the consumer, which reads raw bytes with no deserializer middleware. Both
sides own the contract explicitly rather than delegating it to a framework convention that the two
ends could drift on.

---

## Producer settings, and their optimal values

| Setting | KafkaFlow / config | Optimal | Why |
|---|---|---|---|
| **Acks** | `.WithAcks(Acks.All)` | **All** | A write is acknowledged only once the in-sync replicas hold it. `Leader` loses data if the leader dies before replication; `None` is fire-and-forget. |
| **Idempotence** | `.WithProducerConfig(new ProducerConfig { EnableIdempotence = true })` | **true** | No duplicate on retry, no reordering of a key. This is what makes the consumer's at-least-once projection safe. |
| **max in-flight** | default (`5`) | **leave default** | With idempotence on, librdkafka tracks sequence numbers and preserves per-key order even with 5 requests in flight. Without idempotence you'd need `1` — throughput gone. |
| **Linger** | `.WithLingerMs(5)` | **5–20 ms** | A few ms of batching for a real throughput gain at negligible latency. Raise under heavy volume. |
| **Compression** | `ProducerConfig.CompressionType` | **`Lz4`** (or `Zstd`) | JSON compresses well; `Lz4` is fast, `Zstd` packs tighter. Cuts network and disk. |
| **Partitioner** | default | **do not set** | The default hashes the key → an aggregate always lands on the same partition. A custom or manual partition **breaks ordering**. |
| **Message timeout / retries** | `ProducerConfig` defaults | **defaults** | Default is a 5-minute delivery window with retries; idempotence makes those retries safe. |
| **Security** | `.WithSecurityInformation(...)` | `SaslSsl` prod / `SaslPlaintext` in a VPN, `ScramSha512` | Same posture as the listener. Credentials from env, never source. |

The one setting people reach for and shouldn't is a **custom partitioner** — it feels like control,
and it quietly dismantles the ordering guarantee. Leave partitioning to the key.

---

## The producer, wired

From the sample — the whole configuration:

```csharp
services.AddKafka(kafka => kafka
    .AddCluster(cluster =>
    {
        cluster.WithBrokers(bootstrap.Split(','));

        cluster.WithSecurityInformation(security =>
        {
            security.SecurityProtocol = SecurityProtocol.SaslPlaintext; // SaslSsl over untrusted networks
            security.SaslMechanism = SaslMechanism.ScramSha512;
            security.SaslUsername = username;   // from the environment
            security.SaslPassword = password;   // from the environment
        });

        cluster.AddProducer("attributes-events", producer => producer
            .DefaultTopic(topic)
            .WithAcks(Acks.All)
            .WithProducerConfig(new ProducerConfig { EnableIdempotence = true }));
    }));
```

In a long-running command service, start the bus with the host exactly as the listener does —
`AddKafkaFlowHostedService(...)` instead of building the provider by hand — so producers come up with
the app. The sample starts it manually (`provider.CreateKafkaBus().StartAsync()`) only because it is a
one-shot console program.

And the produce itself, where the contract is enforced:

```csharp
var producer = provider.GetRequiredService<IProducerAccessor>().GetProducer("attributes-events");

var key = Encoding.UTF8.GetBytes(aggregateId.ToString());        // 1. key = AggregateId
var headers = new MessageHeaders();
headers.Add("type", Encoding.UTF8.GetBytes(@event.GetType().Name)); // 2. type header
var value = JsonSerializer.SerializeToUtf8Bytes(                 // 3. camelCase JSON
    @event, @event.GetType(),
    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

await producer.ProduceAsync(topic, key, value, headers);
```

---

## Ordering discipline — the publisher's half

The consumer's ordering guarantee is only as good as the order events are *produced* in.

- **Produce a key's events in version order.** Enqueue v1 before v2 before v3 for a given aggregate.
  With idempotence + `acks=all`, librdkafka preserves that order to the partition even with retries and
  in-flight batching — you don't have to await each message, only enqueue them in order.
- **One aggregate, one producer, one thread.** Never produce the same aggregate's events concurrently
  from two places — that can interleave versions before they reach the partition, and no consumer
  setting recovers from an aggregate that was produced out of order. Different aggregates, of course,
  produce freely in parallel.
- **Delivery confirmation.** `await ProduceAsync(...)` blocks until the broker acks (or throws) — use
  it when each write must be confirmed. For a high-volume stream, prefer fire-and-forget
  `Produce(..., deliveryHandler)` with a callback that inspects `report.Error`, then a final `Flush`;
  handle any delivery error there, because a silently dropped produce is a gap the consumer can never
  see.

---

## The event contract, concretely

Type header value is the event type name; the body is camelCase JSON. The first event of any
aggregate must be `AttributeCreated` at version 1, then contiguous versions.

```jsonc
// header:  type = "AttributeCreated"
{
  "aggregateId": "0d8f…",   // also the message key, as a string
  "version": 1,
  "userId": "…",
  "dateTime": "2026-08-11T11:30:45Z",
  "data": {
    "metadata": {
      "arabicDisplayName": "…",
      "englishDisplayName": "…",
      "arabicDescription": null,
      "englishDescription": null
    },
    "type": "SingleSelect"    // or "MultiSelect"
  }
}
```

```jsonc
// header:  type = "AttributePublished"   — no data body
{ "aggregateId": "0d8f…", "version": 2, "userId": "…", "dateTime": "…" }
```

```jsonc
// header:  type = "AttributeMetadataChanged"
{ "aggregateId": "0d8f…", "version": 3, "userId": "…", "dateTime": "…",
  "data": { "metadata": { "arabicDisplayName": "…", "englishDisplayName": "…" } } }
```

The full set of type names is the consumer's `EventTypeNames`; the JSON shapes are its `Events`
records. In a real command service these would be your own event definitions (or a shared contracts
package) — the wire shape is what has to match, not the code.

---

## Running the sample

```bash
KAFKA_BOOTSTRAP=broker-1:9092,broker-2:9092,broker-3:9092 \
KAFKA_TOPIC=app-events \
KAFKA_USERNAME=app KAFKA_PASSWORD=**** \
dotnet run --project samples/AnisShop.Attributes.Sample.Publisher -- 5
```

Publishes 5 aggregates, each `v1 Created → v2 Published → v3 MetadataChanged`. Point a listener at the
same topic and the read model gains 5 attributes, each at version 3 — which is exactly how this
producer was verified.

---

## Checklist

- [ ] Key set to `AggregateId` (UTF-8 string); partitioner left at default.
- [ ] `type` header on every message = the event type's name.
- [ ] Value is camelCase JSON matching the consumer's records.
- [ ] `Acks.All` and `EnableIdempotence = true`.
- [ ] A key's events enqueued in version order, from one place.
- [ ] Delivery errors handled (awaited, or inspected in the callback) — never dropped silently.
- [ ] SASL credentials from the environment, not `appsettings.json`.
