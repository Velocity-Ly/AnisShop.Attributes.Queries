# AnisShop.Kafka.Sessions

**Azure Service Bus session semantics on Kafka partitions.**

Consume a Kafka topic the way you consume a session-enabled Service Bus queue: one session's messages
arrive in the order the sender produced them and never overlap with themselves, while thousands of
sessions are handled in parallel.

If you know `ServiceBusSessionProcessor`, you know this API. There is one difference, and it is an
improvement: your handler is called with **a run of messages** from one session rather than one
message at a time.

> Step-by-step wiring: [GETTING-STARTED.md](GETTING-STARTED.md). This page is the full reference.

---

## Contents

- [The problem](#the-problem)
- [How it works](#how-it-works)
- [What it deliberately does not do](#what-it-deliberately-does-not-do)
- [The contract](#the-contract)
- [Quick start](#quick-start)
- [API surface](#api-surface)
- [Configuration](#configuration)
- [Guarantees](#guarantees)
- [Failure policy](#failure-policy)
- [Compared with ServiceBusSessionProcessor](#compared-with-servicebussessionprocessor)
- [Operating it](#operating-it)
- [Testing](#testing)
- [Limitations](#limitations)
- [Requirements](#requirements)

---

## The problem

A Service Bus session gives you an **exclusive lock on one session**. `AcceptNextSessionAsync` hands
you a stream that no other receiver in any process can touch, its messages are FIFO, and you can hold
a thousand such locks at once. Ordering is the broker's job and parallelism is just "hold more locks".

Kafka's only ordering unit is the **partition**, and partitions are a scarce, pre-provisioned
resource. A topic with 32 partitions carries every one of your millions of sessions, so a partition
is a *bundle of interleaved sessions*, not a session.

That leaves two bad options:

| Naive approach | Result |
|---|---|
| Consume the partition in order, one message at a time | Correct, but one message in flight per partition |
| Consume it with a thread pool | Fast, but two messages of the same session can be handled out of order |

## How it works

Rebuild the session shape in the consumer. Three moves:

**1. The sender keys every message by its session id.** Kafka's default partitioner hashes the key,
so a session never spans two partitions and its messages sit there in production order. The Kafka key
is your Service Bus `SessionId` and `PartitionKey` collapsed into one field — logical, unbounded, and
set by the sender.

**2. One worker owns one partition.** The consume loop never handles anything; it routes each record
into the buffer of the worker that owns its partition. Partitions drain concurrently, and no
partition is ever touched by two threads. (librdkafka's consumer is not thread-safe, so exactly one
thread ever touches it.)

**3. Each worker regroups its batch by key, then fans out.** A batch of 1000 records off one partition
might hold 400 distinct sessions; grouping turns it back into 400 session-shaped runs, handled
concurrently under a shared gate, each delivered strictly in arrival order.

```
partition 7:  A1 B1 C1 A2 B4 C2 A3 B5 …      ← one ordered log, sessions interleaved
                        │
                  group by message key
                        │
          ┌─────────────┼─────────────┐
       A1 A2 A3      B1 B4 B5      C1 C2       ← sessions, handled in parallel,
                                                 each in sender order
```

The result is the same shape a session receiver gives you. The difference is only *who* does the
grouping: Service Bus does it in the broker, this does it in the worker.

## What it deliberately does not do

This is the important part, and it is what keeps the boundary honest:

- **No sorting.** The order is the order the sender produced. There is no sequence number to sort by.
- **No deduplication.** Delivery is at-least-once, exactly as with a session receiver.
- **No gap detection.** Nothing inspects your payload, so nothing can distinguish a "missing" message
  from one that was never sent.
- **No deserialization.** You get raw `ConsumeResult<string, byte[]>` records — the counterpart of a
  raw `ServiceBusReceivedMessage`.

Whether version 5 may be applied before version 6 has arrived is **business logic**. It belongs in
your consumer, not in a transport. A session receiver does not do it for you, and neither does this.

## The contract

Symmetric, and the same deal sessions offer:

| You guarantee | The package guarantees |
|---|---|
| The sender sets a key on every message | One session never spans two partitions |
| The sender produces a session's messages in the order you want them applied | Your handler receives them in exactly that order |
| Your handler is idempotent | Two calls for the same session are never in flight at once |
| Your handler throws rather than swallowing what it cannot process | Offsets only ever commit below the oldest message you have not finished |

The broker cannot verify that the sender segmented or ordered correctly — same as `SessionId` — and
neither can this. Everything above assumes the sender held up its end.

## Quick start

```csharp
services.AddKafkaSessionProcessor(builder.Configuration);
services.AddHostedService<OrderEventListener>();
```

```csharp
public class OrderEventListener : IHostedService
{
    private readonly KafkaSessionProcessor _processor;

    public OrderEventListener(KafkaSessionProcessor processor)
    {
        _processor = processor;
        _processor.ProcessSessionMessagesAsync += ProcessSessionAsync;
        _processor.ProcessErrorAsync += OnErrorAsync;
    }

    public Task StartAsync(CancellationToken ct) => _processor.StartProcessingAsync(ct);

    public Task StopAsync(CancellationToken ct) => _processor.StopProcessingAsync(ct);

    private async Task ProcessSessionAsync(ProcessSessionMessagesEventArgs args)
    {
        // args.SessionId — the key the sender set.
        // args.Messages  — that session's messages, in production order.
        using var scope = _scopeFactory.CreateScope();
        var readModel = scope.ServiceProvider.GetRequiredService<IOrderReadModel>();

        await readModel.ApplyAsync(
            args.SessionId,
            args.Messages.Select(Deserialize).ToList(),
            args.CancellationToken);
    }

    private Task OnErrorAsync(ProcessSessionErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Session {Id} on {Partition} failed", args.SessionId, args.Partition);
        return Task.CompletedTask;
    }
}
```

| Your handler | Meaning | What happens |
|---|---|---|
| returns | done | the offsets move on |
| throws | not done | the partition blocks and retries with escalating backoff — nothing is discarded |

There is no "abandon", no dead-letter, and no return value to get wrong.

## API surface

Six public types. No interfaces to implement, no generics.

| Type | Purpose |
|---|---|
| `KafkaSessionProcessor` | Owns the consumer. `ProcessSessionMessagesAsync` / `ProcessErrorAsync` events, `StartProcessingAsync` / `StopProcessingAsync`, `IsProcessing`, `IAsyncDisposable` |
| `ProcessSessionMessagesEventArgs` | `SessionId`, `Messages`, `Partition`, `CancellationToken` |
| `ProcessSessionErrorEventArgs` | `Exception`, `SessionId`, `Partition`, `CancellationToken` |
| `KafkaSessionProcessorOptions` | Configuration, with `IValidatableObject` cross-checks |
| `PartitionSessionWorker` | One per partition — batching, grouping, fan-out, blocking, cursor. Public so it can be driven in tests without a broker |
| `OffsetWatermark` | The cursor rule, isolated |

Plus `services.AddKafkaSessionProcessor(configuration, sectionName)`.

Exactly one handler may be registered on each event, and it must be registered before processing
starts — the same restriction `ServiceBusProcessor` imposes, for the same reason: a second handler
that threw would make "did this session succeed?" ambiguous.

## Configuration

```jsonc
{
  "Kafka": {
    "BootstrapServers": "broker-1:9092,broker-2:9092",  // required
    "Topic": "order-events",                            // required
    "ConsumerGroup": "orders-read-model",               // required

    "MaxConcurrentPartitions": 32,
    "MaxConcurrentSessions": 1000,
    "MaxMessagesPerSession": 100,
    "PartitionBufferSize": 4000,
    "BatchLingerMilliseconds": 25,
    "HandlerTimeoutMilliseconds": 60000,
    "RetryBackoffMilliseconds": 200,
    "MaxRetryBackoffMilliseconds": 30000,
    "OffsetCommitIntervalMilliseconds": 5000
  }
}
```

| Setting | Default | What it does |
|---|---|---|
| `MaxConcurrentSessions` | 1000 | Sessions handled at once across every owned partition. **The one to tune.** Size it against your database connection pool, not your partition count |
| `MaxConcurrentPartitions` | 32 | Partitions that may be handling a batch at once. Caps in-flight work and therefore memory |
| `MaxMessagesPerSession` | 100 | Messages of one session per handler call. A session holding more arrives in several back-to-back calls |
| `PartitionBufferSize` | 4000 | Per-partition buffer, and the ceiling on one batch. When it fills, that partition is paused on the broker |
| `BatchLingerMilliseconds` | 25 | Wait for stragglers before handling a partial batch. Fatter batches surface more distinct sessions, which widens the fan-out |
| `HandlerTimeoutMilliseconds` | 60000 | Longest one handler call may run before the attempt is abandoned and retried |
| `RetryBackoffMilliseconds` | 200 | First retry of a blocked partition; doubles per consecutive blocked cycle |
| `MaxRetryBackoffMilliseconds` | 30000 | Ceiling once the backoff has escalated |
| `OffsetCommitIntervalMilliseconds` | 5000 | How often librdkafka flushes stored offsets |

**`MaxConcurrentSessions` must be at least `MaxConcurrentPartitions`**, and this is validated: a
partition that takes a slot and then cannot get a single session slot holds the slot doing nothing,
so partition concurrency would silently drop below what you configured.

Validation runs when the options are first resolved (processor construction), **not** via
`ValidateOnStart`, so a test host that never starts the processor still boots against empty
placeholders filled from secrets at runtime.

Anything librdkafka supports that is not listed — SASL, SSL, timeouts, fetch tuning — goes through
the `configureConsumer` constructor hook:

```csharp
new KafkaSessionProcessor(options, loggerFactory, config =>
{
    config.SecurityProtocol = SecurityProtocol.SaslSsl;
    config.SaslMechanism = SaslMechanism.ScramSha512;
    config.SaslUsername = username;
    config.SaslPassword = password;
});
```

The offset settings the guarantee depends on are re-applied after your hook and cannot be
overridden.

## Guarantees

### Ordering

One session's messages are delivered in production order, and **two calls for the same session are
never in flight at once**. Different sessions run in parallel. A session larger than
`MaxMessagesPerSession` is split into several calls, issued back to back, still in order.

### Concurrency

Two gates: `MaxConcurrentPartitions` bounds how many partitions are handling a batch, and
`MaxConcurrentSessions` bounds how many session runs are in flight globally. Backpressure is
per-partition — when a worker's buffer fills, **only** that partition is paused on the broker, so a
slow session never starves its neighbours.

### Delivery and offsets

At-least-once. A partition has one cursor but sessions finish out of order, so the cursor may only
move to just below the **oldest unfinished message**:

> If offsets 0–9 are done except 4, the stored position is **4**, not 10.

Offsets 5–9 are re-read after a restart — which is why your handler must be idempotent. The consumer
runs `EnableAutoCommit = true` with `EnableAutoOffsetStore = false`: librdkafka commits in the
background, cheaply and off the hot path, but only ever commits positions the workers explicitly
stored after a handler returned. **A commit can never run ahead of your handler.**

Positions are handed from the workers back to the single consume thread through a queue, because
librdkafka's consumer is not thread-safe.

### Rebalancing

Cooperative-sticky, so adding or losing an instance only moves the partitions that actually change
hands. **Revoked** partitions drain their workers (bounded wait, 30s) and commit what they finished.
**Lost** partitions stop and commit nothing — the assignment is already gone and committing would
clobber the new owner. Positions belonging to partitions you still own survive either way.

## Failure policy

**Nothing is ever discarded.** There is no dead-letter topic and no skip path. A message that cannot
be processed blocks its partition until it can be, and the cursor never moves past it. The design
assumes blocks do not happen; when one does, it is loud and it is safe, and no operator has to
reconstruct what was dropped.

**Blocked means stopped, not merely uncommitted.** The worker pulls nothing new, retries exactly the
messages that failed, and its buffer fills until the processor pauses that partition on the broker. A
perfectly good message behind a blockage is not consumed at all. Sessions already *inside* the
in-flight batch still finish in parallel — blocking is applied between batches by refusing new work,
never by serialising the fan-out.

**Partial progress is kept.** If a session's third call throws, the first two are done and are not
repeated; the retry set starts at the failing call.

**Retries escalate.** `RetryBackoffMilliseconds` doubles per consecutive blocked cycle up to
`MaxRetryBackoffMilliseconds`, so a brief fault recovers in milliseconds while a long outage settles
into one attempt every 30s instead of a hot loop.

| Log level | When |
|---|---|
| `Critical` | The first blocked cycle, with the exact partition and offset |
| `Warning` | Each subsequent blocked cycle |
| `Information` | Recovery |

Most blockages resolve themselves — a database returning, or a deploy that teaches your handler a
message type it did not know. The bytes are still in the partition, so a deploy drains the backlog
on its own, in order, with nothing replayed by hand. That is the whole argument for blocking over
dead-lettering.

### The cost, stated plainly

One unprocessable message stops one partition, and **every session sharing that partition stops with
it**. Consumer lag grows without bound until someone intervenes. That is the accepted trade:
correctness and recoverability over availability, on the assumption that blocks do not happen.

## Compared with `ServiceBusSessionProcessor`

| | Service Bus sessions | This package |
|---|---|---|
| Session id | `SessionId`, set by the sender | The **message key**, set by the sender |
| Lock granularity | One session | One partition (regrouped into sessions in the worker) |
| Handler receives | One message | **A run of one session's messages** |
| Ordering source | Broker FIFO within the session | Partition order, regrouped by key |
| Concurrency knob | `MaxConcurrentSessions` | `MaxConcurrentSessions` + `MaxConcurrentPartitions` |
| Batch knob | `MaxMessagesPerSession` | `MaxMessagesPerSession` |
| Delivery | At-least-once | At-least-once, but a **tail** is replayed rather than specific messages |
| Failure | Abandon / dead-letter | Throw → block and retry forever |
| Scale-out ceiling | None practical | **The partition count** |
| Unsessioned message | Rejected at send time | Accepted by Kafka; delivered as one empty-id session with a warning |

Three differences deserve emphasis:

- **Partition count is a hard ceiling on consumer instances.** 32 partitions means at most 32 useful
  consumers, however many pods run. Provision generously — partitions can be added later, but adding
  them re-hashes keys, so a key can change partition and its ordering breaks across the change.
- **Replay is coarser.** Service Bus redelivers exactly the messages you did not complete; Kafka
  re-reads from the cursor, so a tail of already-handled messages comes back. Idempotency absorbs
  this, but the read amplification after a rebalance is real.
- **Blocking is coarser.** A blocked Service Bus session stops one session; the other 999 carry on. A
  blocked Kafka partition takes every session on it down with it, because they share one cursor.

## Operating it

**Scale out** by adding instances to the consumer group; each is assigned a disjoint slice of the
partitions. **Scale up** inside a process with `MaxConcurrentSessions`.

**Alert on exactly two things:**

1. **Consumer lag** on any partition — your primary signal.
2. **The `Critical` log line** — `"{Partition} is blocked at offset {Offset}"`. It names the exact
   partition and offset to investigate.

**Key every message.** A message with no key has no session and no ordering guarantee anywhere in
Kafka. Such messages are delivered as a single empty-id session — never discarded, never silently
parallelised — and logged as a warning once per partition.

## Testing

The package ships **21 broker-free tests**. `ConsumeResult<string, byte[]>` is a plain object, so a
partition is faked by handing out sequential offsets in append order, and `PartitionSessionWorker` is
driven directly with a fake handler — no host, no DI container, no broker.

Notable cases, because they pin behaviour that is easy to assume wrongly:

| Test | Proves |
|---|---|
| `Handle_SessionsInOneBatch_RunConcurrently` | Five calls block until all five are in flight — a serialised implementation could never finish. The scalability claim, without a timing guess |
| `Handle_OneSession_NeverOverlapsWithItself` | The other half of the session guarantee: parallelism is *between* sessions only |
| `Handle_RedeliveredMessages_AreHandedOverAgain` | There is **no** transport-level deduplication, so nobody assumes there is |
| `Handle_HandlerRecovers_DrainsTheBacklogInOrder` | Blocking unblocks itself with the backlog intact |
| `Handle_FullBuffer_StopsAcceptingRecords` | Backpressure engages without spinning |

**Testing your own handler** is easy for the same reason: it is an ordinary async method taking
`ProcessSessionMessagesEventArgs`. Call it directly, or construct a `PartitionSessionWorker` around
it and feed it fake records to exercise ordering and blocking end to end.

## Limitations

- **One topic per registration.** `AddKafkaSessionProcessor` binds a single unnamed options instance,
  so calling it twice makes both processors share one configuration. For a second topic in the same
  process, construct a `KafkaSessionProcessor` directly with its own options object.
- **Key and value types are fixed** to `string` and `byte[]`. Schema-registry deserializers must run
  inside your handler.
- **Not yet run against a real broker.** The ordering, grouping, blocking, backpressure and cursor
  logic are covered by the test suite; the consumer configuration, rebalance callbacks and
  pause/resume semantics have not been exercised end to end against a live cluster.

## Requirements

- .NET 10.0
- Confluent.Kafka 2.15.0
- A topic whose messages are **keyed by session id**
