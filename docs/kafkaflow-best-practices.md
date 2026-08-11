# KafkaFlow — configuration & best practices

> **New to KafkaFlow here?** Start at [kafkaflow.md](kafkaflow.md) — the single entry point.
>
> **Purpose**: a practical guide for wiring the KafkaFlow listener correctly. Every setting, its
> optimal value (or the range to choose from), and the handful of rules you cannot break without
> losing ordering or the read model. If you only read one section, read [The one rule](#the-one-rule-everything-else-depends-on-it).
>
> For *why* the listener is built the way it is, read [`kafkaflow-listener.md`](kafkaflow-listener.md).
> This document is how to run it. For the **producing** side — how a command service should publish so
> these guarantees hold — read [`kafkaflow-publisher.md`](kafkaflow-publisher.md).

---

## The one rule everything else depends on it

**The Kafka message key must be the `AggregateId`.** This is the publisher's job, and every guarantee
below is void without it.

KafkaFlow routes a message to a worker by hashing its key (`BytesSumDistributionStrategy`). Same key →
same worker → processed one at a time → **ordered**. Two different aggregates get different workers
and run in parallel. So:

- Key set to `AggregateId` → each aggregate is strictly ordered, aggregates scale independently. ✅
- Key null or set to anything else → all events pile onto one worker (a null key hashes to worker 0),
  or worse, one aggregate's events scatter across workers and **reorder**. ❌

There is no setting that recovers ordering if the key is wrong. Verify the publisher first — see
[`kafkaflow-publisher.md`](kafkaflow-publisher.md) for how to produce this correctly.

---

## Quickstart

Select the transport and point it at the cluster. Non-secret values go in `appsettings.json` (or per
environment); **credentials come from environment variables**, never source control.

```jsonc
// appsettings.json — safe to commit
{
  "Messaging": { "Transport": "KafkaFlow" },
  "KafkaFlow": {
    "BootstrapServers": "broker-1:9092,broker-2:9092,broker-3:9092",
    "Topic": "app-events",
    "ConsumerGroup": "app-attributes-queries",
    "WorkersCount": 16,
    "BufferSize": 100,
    "BatchSize": 100,
    "BatchTimeoutMilliseconds": 25,
    "SecurityProtocol": "SaslPlaintext",
    "SaslMechanism": "ScramSha512"
    // SaslUsername / SaslPassword deliberately absent — see Security
  }
}
```

```bash
# credentials — from the environment, a secret store, or user-secrets. NEVER appsettings.json.
KafkaFlow__SaslUsername=app
KafkaFlow__SaslPassword=********
```

The double underscore `__` is the .NET configuration separator, so `KafkaFlow__SaslPassword` overrides
`KafkaFlow:SaslPassword`. Anything in `appsettings.json` can be overridden the same way per
environment.

---

## Every setting, and its optimal value

| Setting | Default | Optimal / range | Notes |
|---|---|---|---|
| `Messaging:Transport` | `ServiceBus` | `KafkaFlow` | Selects this listener. Exactly one transport runs. |
| `BootstrapServers` | — (required) | **list ≥ 2–3 brokers** | Discovery seed only; the client learns the rest. Listing one broker means a single point of failure *at startup*. |
| `Topic` | — (required) | the publisher's event topic | Must match where events are actually produced. |
| `ConsumerGroup` | — (required) | stable, one per service, within the ACL prefix | See [Consumer group](#consumer-group-the-setting-people-get-wrong). |
| `WorkersCount` | 32 | **16–32** for a SQL projection | The per-instance parallelism. Size it to the *database*, not the CPU. See [Scale](#scaling-instances--workerscount). |
| `BufferSize` | 100 | **100–200** | Messages prefetched per worker. Higher smooths throughput but replays more on rebalance. |
| `BatchSize` | 100 | **100–500** | Events per projection call (one DB transaction). Higher = fewer round trips, **bigger poison blast radius**. |
| `BatchTimeoutMilliseconds` | 25 | **25–100** | Max wait to fill a batch. Lower = lower latency; higher = fuller batches under bursty load. |
| `SecurityProtocol` | `Plaintext` | `SaslSsl` prod / `SaslPlaintext` inside a VPN | Never `Plaintext` outside local dev. |
| `SaslMechanism` | `ScramSha512` | **`ScramSha512`** | `ScramSha256` fine. Avoid `Plain` unless over TLS. |
| `SaslUsername` / `SaslPassword` | — | env vars / secret store | Required when the protocol is SASL; validated at startup. Never committed. |
| `IgnoreUnrecognizedMessages` | `false` | **`false`** on a dedicated topic | `true` **only** on a topic shared with foreign producers. See [Poison](#the-poison-message-contract). |

`AutoOffsetReset` is fixed to `Earliest` in code and is not a setting: a projection that has never run
must rebuild from the start of the log. A consequence — **changing `ConsumerGroup` triggers a full
replay**, because a new group has no committed offset.

`BootstrapServers`, `Topic` and `ConsumerGroup` are `[Required]` and validated **at registration**,
not lazily — KafkaFlow builds its whole topology during `AddKafka`. A missing SASL credential fails
the same way, at startup, with a named error, rather than as a broker-transport timeout ten seconds
in.

---

## Consumer group, the setting people get wrong

One group name, shared by **every instance** of this service. Kafka splits the topic's partitions
among the group's members; add an instance and partitions redistribute to it. That is how you scale.

Three ways to get it wrong:

1. **A unique group per instance** (e.g. a GUID suffix). Each instance becomes its own group and
   receives **all** partitions, so every event is projected N times. The read model's idempotency
   hides the duplication until it doesn't. Use one stable name.
2. **A name outside the ACL grant.** Managed clusters authorize a *prefix* (e.g. `app-`). A group
   named `attributes-queries` when the grant is `app-*` fails with `GroupAuthorizationFailed` — a
   coordinator error several seconds into startup, not an obvious "access denied". Name it
   `app-attributes-queries`.
3. **Renaming it casually.** A new name is a new consumer with no committed offset, so it replays the
   entire topic from `Earliest`. Fine on purpose, painful by accident.

---

## Scaling: instances × WorkersCount

The parallelism ceiling is:

```
effective lanes = min(instances, partitions) × WorkersCount
```

- **`WorkersCount`** is per-instance concurrency: at most this many aggregates project at once on one
  instance, each holding one database transaction. Size it to what the read-model database can absorb
  without deadlocking — for SQL Server, start at **16–32** (the default connection pool is 100). Past
  the DB's write ceiling, more workers only add contention.
- **Instances** scale horizontally, but an instance only does work if it owns a partition. **You can
  never have more useful instances than partitions.** With 12 partitions, instance 13 sits idle.
- To raise the ceiling: add instances up to the partition count, then raise `WorkersCount` until the
  database is the bottleneck.

> **Partition count is a capacity decision to make up front.** Increasing partitions later changes the
> key→partition mapping for new messages, so an aggregate's future events can land on a different
> partition than its past ones — breaking per-aggregate ordering across the change. Over-provision
> partitions at creation; do not repartition a keyed event topic in place.

How this differs from the Service Bus session model is the one trade-off worth internalising: Service
Bus hands out up to 1000 sessions dynamically, any aggregate to any handler. KafkaFlow's lanes are
**fixed hash buckets** — two hot aggregates that hash to the same worker serialise behind each other
even while other workers idle. With enough workers relative to active aggregates this is invisible;
know it's there before you tune `WorkersCount` down.

---

## Security

- **Inside a VPN / trusted network:** `SaslPlaintext` + `ScramSha512`. The tunnel provides transport
  security; SASL provides authentication. This is a valid production posture on a private network.
- **Over an untrusted network:** `SaslSsl` — authentication *and* encryption.
- **Local dev only:** `Plaintext`.

Credentials never live in `appsettings.json` or git. Supply `KafkaFlow__SaslUsername` /
`KafkaFlow__SaslPassword` through environment variables, a secret store, or
`dotnet user-secrets` in development. If the protocol is SASL and either credential is missing,
startup fails immediately with a message naming the missing value — by design, so a credential
mistake never looks like a network fault.

---

## The poison-message contract

Know exactly what happens to a message the projector cannot turn into an event, because the default is
deliberately unforgiving.

- A message with a **known `type` header that fails to deserialize** → the projector throws. KafkaFlow
  logs it and completes the **whole worker batch**, so that message *and every other aggregate in the
  batch* are skipped — a permanent hole in the read model. `KafkaFlowEventProjector.Fail` logs
  `Critical` with the aggregate id, partition and offsets first, because that log line is the only
  record of what was lost. **Alert on `Critical` from this logger.** This is intentional: a silent
  hole is worse than a loud stop.
- A message with **no recognised `type` header** is handled by `IgnoreUnrecognizedMessages`:
  - `false` (default, and correct for a **dedicated** event topic): treated as the contract violation
    it is → same throw as above.
  - `true` (only for a topic **shared** with other producers — smoke tests, health probes): the
    foreign message is skipped with a debug log, and real events project normally.

Set `IgnoreUnrecognizedMessages: true` **only** when you knowingly share the topic. On a topic that
carries nothing but your events, a header-less message means something is wrong and you want to hear
about it.

---

## Ordering, rebalance, and idempotency

- **Ordering** holds per aggregate because the key pins each aggregate to one worker. It survives a
  partition moving between instances during a rebalance: the new owner resumes the partition from the
  last committed offset, in order.
- **At-least-once, so be idempotent.** KafkaFlow commits an offset only after a batch completes
  cleanly, so a rebalance or restart **replays** whatever was in flight. The projection must absorb a
  replayed event without double-applying it. The `IncomingEvents` handler already does: it skips any
  event whose version is `≤` the aggregate's current version, and refuses (does not silently accept) a
  version gap. Any projection you add must keep that property.
- **The version-contiguity check is your correctness oracle.** The handler applies an event only if
  its version is exactly `current + 1`. So if every aggregate reaches its expected final version and
  no `Critical` line appears, ordering held end to end. That is the single most useful thing to assert
  in any test or load run.

---

## Running and verifying against a cluster

Point the built app at the cluster with environment variables (nothing secret on disk):

```bash
Messaging__Transport=KafkaFlow
KafkaFlow__BootstrapServers=broker-1:9092,broker-2:9092,broker-3:9092
KafkaFlow__Topic=app-events
KafkaFlow__ConsumerGroup=app-attributes-queries
KafkaFlow__SecurityProtocol=SaslPlaintext
KafkaFlow__SaslMechanism=ScramSha512
KafkaFlow__SaslUsername=app
KafkaFlow__SaslPassword=********
ConnectionStrings__AttributesDatabase=Server=...;Database=...;TrustServerCertificate=True
```

`DatabaseRunner` migrates the read-model database on startup, so a fresh database needs no manual
schema step.

To verify a run is healthy:

1. **No `Critical` logs** from `KafkaFlowEventProjector` — zero poison, zero ordering violations.
2. **Every aggregate at its expected version** in the read model. `SELECT MIN(Version), MAX(Version)`
   over a load of uniform-length streams should show a single value: proof that every event was
   applied, in order.
3. **A forced rebalance changes neither.** Start a second instance on the same group mid-load; the
   two above must still hold.

---

## Checklist

- [ ] Publisher sets the Kafka key to `AggregateId`.
- [ ] `ConsumerGroup` is one stable name, shared by all instances, inside the ACL prefix.
- [ ] `WorkersCount` sized to the database's write concurrency (16–32 to start), not the CPU.
- [ ] Partition count provisioned generously up front — never repartitioned in place.
- [ ] `SecurityProtocol` is SASL; credentials come from env/secret store, not `appsettings.json`.
- [ ] `IgnoreUnrecognizedMessages` left `false` unless the topic is deliberately shared.
- [ ] Alerting wired to `Critical` from `KafkaFlowEventProjector`.
- [ ] Any new projection is idempotent per `(aggregate, version)`.
