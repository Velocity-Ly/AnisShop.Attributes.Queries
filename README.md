# AnisShop.Attributes.Queries

The **query side** of the product-attributes bounded context (CQRS). It consumes domain events
published by the command side, projects them into a flat relational **read model**, and serves that
model over a **gRPC** API. It holds no business rules — only the ordered application of events and the
queries over the result.

```
 command side ──events──▶  [ transport ]  ──▶  IncomingEvents projection  ──▶  read model (SQL Server)
                            ServiceBus                                              │
                            Kafka                                                   ▼
                            KafkaFlow                                        gRPC query API
```

## What it does

- **Projects events** — `AttributeCreated`, `AttributePublished`, `AttributeMetadataChanged`, option
  and category changes, deprecation/disable, delete — into an `Attribute` read model with its options
  and applicable categories. Projection is **idempotent per `(AggregateId, Version)`** and refuses a
  version gap, so ordering is enforced, not assumed.
- **Serves queries** over gRPC:
  - `Get(id)` — one attribute with its options and categories.
  - `GetByCategory(categoryId, currentPage, pageSize)` — a paginated list within a category.

## Event transport is pluggable

One setting, `Messaging:Transport`, selects how events arrive. All three land in the **same**
`IncomingEvents` projection, so the transport is a deployment choice, not an architectural one.

| `Messaging:Transport` | Consumer | Notes |
|---|---|---|
| `ServiceBus` (default) | Azure Service Bus sessions | ordering via `SessionId = AggregateId` |
| `Kafka` | `AnisShop.Kafka.Sessions` (in this repo) | a hand-rolled session-shaped Kafka consumer |
| `KafkaFlow` | the [KafkaFlow](https://github.com/Farfetch/kafkaflow) package | ordering via key-hashed workers |

### Using KafkaFlow? → **[docs/kafkaflow.md](docs/kafkaflow.md)**

That page is the single entry point for everything KafkaFlow in this codebase — it routes you to the
consumer guide, the publisher guide, and a runnable producer sample, each with every setting's optimal
value. **Start there** before configuring a listener or a publisher.

## Getting started

**Prerequisites**: [.NET 10 SDK](https://dotnet.microsoft.com/), and SQL Server (LocalDB, SQL Express,
or a full instance) for the read model.

```bash
# build and test
dotnet build AnisShop.Attributes.Queries.slnx
dotnet test

# run the service (Service Bus is the default transport)
ConnectionStrings__AttributesDatabase="Server=.\SQLEXPRESS;Database=AnisShopAttributes;TrustServerCertificate=True" \
dotnet run --project src/AnisShop.Attributes.Queries
```

The database schema is created and migrated automatically on startup (`DatabaseRunner`), so a fresh
database needs no manual step. Secrets — connection strings, SASL credentials — come from the
environment or a secret store, never `appsettings.json`.

To run against Kafka/KafkaFlow instead, set `Messaging__Transport` and the matching section; the
[KafkaFlow entry point](docs/kafkaflow.md) has copy-paste commands.

## Project layout

| Path | What it is |
|---|---|
| `src/AnisShop.Attributes.Queries` | the service: projection, read model, gRPC API, and the three transports under `Infrastructure/` |
| `src/AnisShop.Kafka.Sessions` | a standalone, session-shaped Kafka consumer library (the `Kafka` transport) |
| `samples/AnisShop.Attributes.Sample.Publisher` | a reference **KafkaFlow producer**, verified against a real broker |
| `test/…` | unit tests, integration tests (SQL LocalDB), and the Kafka.Sessions test suite |
| `docs/` | architecture and usage documentation |

## Documentation

| Doc | About |
|---|---|
| **[docs/kafkaflow.md](docs/kafkaflow.md)** | **KafkaFlow entry point** — consume, publish, and the design, each with a sample |
| [docs/kafkaflow-best-practices.md](docs/kafkaflow-best-practices.md) | KafkaFlow **consumer** config, every setting's optimal value |
| [docs/kafkaflow-publisher.md](docs/kafkaflow-publisher.md) | KafkaFlow **producer** config, with the sample |
| [docs/kafkaflow-listener.md](docs/kafkaflow-listener.md) | KafkaFlow design rationale and the real-broker run |
| [docs/kafka-listener.md](docs/kafka-listener.md) | the hand-rolled `AnisShop.Kafka.Sessions` transport |
| [docs/commands-queries-relationship.md](docs/commands-queries-relationship.md) | how the command and query sides relate |

## Tech stack

.NET 10 · gRPC · EF Core 10 / SQL Server · [Mediator](https://github.com/martinothamar/Mediator)
(source-generated) · FluentValidation · Polly · Serilog · Confluent.Kafka · KafkaFlow ·
Azure.Messaging.ServiceBus.
