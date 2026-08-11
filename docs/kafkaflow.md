# KafkaFlow at AnisShop — start here

This service is the **query side**: it consumes domain events from Kafka with
[KafkaFlow](https://github.com/Farfetch/kafkaflow) and projects them into a read model. The **command
side** publishes those events. This page is the single entry point for both — start with the row that
matches what you're doing, then follow it to the full guide and a runnable example.

## What are you doing?

| I want to… | Read this | Example / code |
|---|---|---|
| **Consume events** — configure, run, or tune a listener | [kafkaflow-best-practices.md](kafkaflow-best-practices.md) | listener: [`src/…/Infrastructure/KafkaFlowTransport`](../src/AnisShop.Attributes.Queries/Infrastructure/KafkaFlowTransport) · usage in tests: [`test/…/KafkaFlowTransport`](../test/AnisShop.Attributes.Queries.Tests/KafkaFlowTransport) |
| **Publish events** — produce so the listener accepts them | [kafkaflow-publisher.md](kafkaflow-publisher.md) | sample: [`samples/AnisShop.Attributes.Sample.Publisher`](../samples/AnisShop.Attributes.Sample.Publisher) |
| **Understand the design** — why it's built this way, and the real-broker results | [kafkaflow-listener.md](kafkaflow-listener.md) | — |

If you only have five minutes and you're wiring a producer or a consumer for the first time, read the
matching best-practices page top to bottom — each one ends in a checklist.

## The contract both sides must agree on

Every message carries three things. The publisher sets them; the consumer reads them; a mismatch in
any one is a **poison message**.

1. **Key = `AggregateId`** (UTF-8 string) — one aggregate stays on one partition and one worker, which
   is the entire ordering guarantee.
2. **`type` header** = the event type's name (`AttributeCreated`, …) — the consumer has no other way to
   know what to deserialize.
3. **Value** = the event as **camelCase** JSON.

That's it. Everything in the two best-practices pages is in service of getting these three right and
keeping them right under load and rebalances.

## The mental model in 30 seconds

- **Ordering** comes from the key, not the partition: same key → same worker → processed in order.
  Different aggregates run in parallel.
- **Scale** is `min(instances, partitions) × WorkersCount`. Add instances up to the partition count,
  then raise `WorkersCount` until the database is the bottleneck. Pick the partition count generously
  up front — you can't repartition a keyed topic without breaking ordering.
- **Failure is loud by default**: a message the consumer can't read fails the batch and logs
  `Critical`, rather than leaving a silent hole. On a topic shared with other producers, the opt-in
  `IgnoreUnrecognizedMessages` skips foreign traffic instead — off unless you need it.

## Run something in two minutes

Publish a few events with the sample producer:

```bash
KAFKA_BOOTSTRAP=broker-1:9092,broker-2:9092,broker-3:9092 \
KAFKA_TOPIC=app-events  KAFKA_USERNAME=app  KAFKA_PASSWORD=**** \
dotnet run --project samples/AnisShop.Attributes.Sample.Publisher -- 5
```

Run a listener — it's this service with the transport switched on (credentials from the environment,
never `appsettings.json`):

```bash
Messaging__Transport=KafkaFlow \
KafkaFlow__BootstrapServers=broker-1:9092,broker-2:9092,broker-3:9092 \
KafkaFlow__Topic=app-events  KafkaFlow__ConsumerGroup=app-attributes-queries \
KafkaFlow__SecurityProtocol=SaslPlaintext  KafkaFlow__SaslMechanism=ScramSha512 \
KafkaFlow__SaslUsername=app  KafkaFlow__SaslPassword=**** \
ConnectionStrings__AttributesDatabase="Server=…;Database=…;TrustServerCertificate=True" \
dotnet run --project src/AnisShop.Attributes.Queries
```

Point both at the same topic and watch the read model gain one row per aggregate, each at its final
version.

## The full map

| Document | What it's for |
|---|---|
| [kafkaflow.md](kafkaflow.md) | this page — the entry point |
| [kafkaflow-best-practices.md](kafkaflow-best-practices.md) | **consumer** config & best practices, every setting's optimal value |
| [kafkaflow-publisher.md](kafkaflow-publisher.md) | **producer** config & best practices, with the sample |
| [kafkaflow-listener.md](kafkaflow-listener.md) | design rationale, comparison to the hand-rolled transport, and the real-broker run |

| Code | What it is |
|---|---|
| [`src/…/Infrastructure/KafkaFlowTransport`](../src/AnisShop.Attributes.Queries/Infrastructure/KafkaFlowTransport) | the listener — one middleware, one projector, options |
| [`samples/AnisShop.Attributes.Sample.Publisher`](../samples/AnisShop.Attributes.Sample.Publisher) | the reference producer, verified against a real broker |
| [`test/…/KafkaFlowTransport`](../test/AnisShop.Attributes.Queries.Tests/KafkaFlowTransport) | the listener's behaviour, pinned as tests you can read as examples |
