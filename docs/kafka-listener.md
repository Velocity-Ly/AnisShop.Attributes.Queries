# Kafka Listener — Session-Equivalent Consumption on Partitions

> **Purpose**: Explain how the Kafka transport reproduces the Service Bus session listener's
> guarantees (ordered per aggregate, massively parallel across aggregates) on a broker that has no
> sessions. Three transports are in the tree; `Messaging:Transport` picks one.
>
> The machinery lives in **`src/AnisShop.Kafka.Sessions`**, a standalone project consumed as a
> package. Its README and GETTING-STARTED are the reference for using it anywhere else; this page is
> about how *this* service uses it.
>
> A third transport reads the same topic through the KafkaFlow package instead of this one. See
> [`kafkaflow-listener.md`](kafkaflow-listener.md) for what that buys and what it costs.

---

## The problem

The Service Bus listener is built on **sessions**, and `SessionId == AggregateId`. That single fact
buys everything:

- `AcceptNextSessionAsync` hands out an **exclusive lock on one aggregate**. No other receiver, in
  any process, can touch it until the lock is released.
- Inside the session, messages are FIFO, so one aggregate's events arrive in the order Commands
  published them.
- Up to `MaxConcurrentSessions` (1000) locks are held at once, each draining up to
  `MaxMessagesPerSession` (100) messages in a bulk receive.

Kafka has no sessions. Its only ordering unit is the **partition**, and partitions are a scarce,
pre-provisioned resource: a topic with 32 partitions carries every one of your millions of
aggregates, so a partition is a *bundle of interleaved sessions*, not a session.

Consume a partition naively — one message at a time, in order — and ordering is correct but
throughput collapses. Consume it with a thread pool and two events of the same aggregate can be
applied out of order.

## The answer

Rebuild the session shape in the consumer. Three parts:

**1. The publisher keys every message by `AggregateId`.**
Kafka's default partitioner hashes the key, so an aggregate never spans two partitions and its
events sit in that partition in publish order. This is the one thing Commands must do; without it
nothing below holds. It is the same premise `SessionId` rests on — the broker cannot verify it, and
neither can we.

**2. One worker owns one partition** (`PartitionSessionWorker`).
The consume loop never handles anything — it routes each record into the buffer of the worker that
owns its partition. Partitions therefore drain concurrently, and no partition is ever touched by two
threads.

**3. The worker regroups the batch by key, then fans out.**
This is the step that recreates sessions. A batch of, say, 1000 records off one partition might
contain 400 distinct aggregates; grouping turns it back into 400 session-shaped runs. Every run is
handled **concurrently** with the others (bounded by `MaxConcurrentSessions`), and every run is
delivered **in publish order**, never concurrently with itself.

```
partition 7:  A1 B1 C1 A2 B4 C2 A3 B5 …      ← one ordered log, sessions interleaved
                        │
                  group by message key
                        │
          ┌─────────────┼─────────────┐
       A1 A2 A3      B1 B4 B5      C1 C2       ← sessions, handled in parallel
```

Result: **ordered per aggregate, parallel across aggregates and across partitions** — the same shape
as the session listener. The difference is only *who* does the grouping: Service Bus does it in the
broker, Kafka does it in the worker.

## What the package deliberately does not do

The first version of this transport sorted each session's events by `Version`, dropped replays and
refused to deliver anything past a gap. That was **wrong at the boundary**: a session receiver does
none of those things. It hands you what the sender sent, in the order the sender sent it, and the
meaning of the payload is your business.

So the package does not sort, does not deduplicate, does not detect gaps, and does not deserialize.
It delivers raw `ConsumeResult<string, byte[]>` records grouped by key — the exact counterpart of a
raw `ServiceBusReceivedMessage` arriving on a locked session.

Everything about *versions* now lives in `KafkaEventListener`, where the equivalent Service Bus
logic already lived.

## The hard part: one cursor, out-of-order work

A session is acknowledged per message. A Kafka partition has **one cursor**, and committing it says
"everything below this is done". But we deliberately finish sessions out of order — session B's run
may complete while session A's is still running.

The rule (`OffsetWatermark.TryAdvance`) is therefore:

> The cursor may only move to just below the **oldest message we have not finished with**.

If offsets 0–9 are handled except offset 4, the stored position is **4**, not 10. Offsets 5–9 get
re-read after a restart and re-delivered — which is free, because `IncomingEventsHandler` skips any
event whose `Version <= currentVersion`. This is the same at-least-once contract the Service Bus
path already relies on for redelivery; nothing new is assumed.

Offsets are handed back from the workers through a queue and stored by the consume thread, because
librdkafka's consumer is **not thread-safe**. The consumer is configured
`EnableAutoCommit = true` + `EnableAutoOffsetStore = false`: librdkafka commits in the background
(cheap, off the hot path) but only ever commits positions we explicitly stored after a handler
returned, so a commit can never run ahead of the read model.

## Failure policy: nothing is ever discarded

There is no dead letter topic and no skip path. A message that cannot be processed **blocks its
partition** until it can be, and the cursor never moves past it. The design assumes blocks do not
happen; when one does, it is loud and it is safe, and no operator has to reconstruct what was
dropped.

With the version logic gone, the package has exactly one failure signal: **the handler throws**.
Blocked means stopped, not merely uncommitted — the worker pulls nothing new, its buffer fills, and
the processor pauses that partition on the broker. Sessions already *inside* the in-flight batch
still finish in parallel; blocking is applied between batches, by refusing new work, never by
serialising the fan-out.

Retries use an escalating backoff — `RetryBackoffMilliseconds` doubling per consecutive blocked
cycle up to `MaxRetryBackoffMilliseconds` — so a brief fault recovers in milliseconds while a long
outage settles into one attempt every 30s instead of a hot loop. The first blocked cycle logs
`Critical` with the exact partition and offset; subsequent ones log `Warning`, and recovery logs
`Information`.

`KafkaEventListener` throws in exactly two cases:

| Case | Why it is a throw rather than a skip |
|---|---|
| The payload will not deserialize | Skipping would leave a hole in the read model that nothing downstream could detect. The bytes stay in the partition, so the deploy that adds the missing event type drains the backlog on its own, in order. |
| `IncomingEventsHandler` returns `false` (version gap) | Under the ordering Commands promises this cannot happen — a session's events arrive in publish order, so version N-1 was handled before N. Reaching here means the promise was broken, and that must be loud. |

Rebalancing is unaffected: revoked partitions drain their workers (bounded wait) and commit what
they finished, while lost partitions stop and commit nothing, since the assignment is already gone
and committing would clobber the new owner.

### The cost, stated plainly

One unprocessable message stops one partition, and every aggregate that shares that partition stops
with it. Consumer lag on that partition grows without bound until someone intervenes. That is the
accepted trade: correctness and recoverability over availability, on the assumption that blocks do
not happen. Alert on consumer lag and on the `Critical` line — those are the only signals that a
partition has stopped.

## Scaling

Three knobs, and the constraint between them:

- **`MaxConcurrentSessions`** (default 1000) — the direct counterpart of `MaxConcurrentSessions` on
  the Service Bus side: how many aggregates are projected at once across every owned partition.
- **`MaxConcurrentPartitions`** (default 32) — how many partitions may be handling a batch at once.
  No Service Bus equivalent; it caps in-flight work and therefore memory. Must be **≤**
  `MaxConcurrentSessions`, which is validated: a partition holding a slot it cannot use would
  silently reduce partition concurrency.
- **`MaxMessagesPerSession`** (default 100) — the counterpart of the Service Bus setting of the same
  name, and the one place this API improves on `ServiceBusSessionProcessor`: it calls you with one
  message, this calls you with up to this many from one session.

Across processes, the consumer group is the other axis: each instance is assigned a disjoint slice of
the partitions, so adding pods multiplies throughput up to the partition count. That is the ceiling
Service Bus does not have, and it is why partition count should be provisioned generously —
partitions can be added later, but adding them re-hashes keys.

## What is genuinely different from Service Bus

- **Partition count is a hard ceiling on consumer instances.** 32 partitions means at most 32 useful
  instances, no matter how many pods run. Session receivers have no such cap.
- **At-least-once is coarser.** Service Bus redelivers exactly the messages that were not completed;
  Kafka re-reads from the cursor, so a tail of already-projected events is replayed. Idempotent
  projection absorbs this, but the read amplification is real after a rebalance.
- **Blocking is coarser too.** A Service Bus session that cannot be processed blocks one aggregate;
  the other 999 sessions carry on. A blocked Kafka partition takes every aggregate on it down with
  it, because they share one cursor. This is the direct cost of trading sessions for partitions, and
  it is why the Service Bus listener still has a dead-letter path while this one does not.
- **A message with no key has no session at all.** The package delivers those as a single empty-id
  session and logs a warning. Service Bus rejects an unsessioned message at send time; Kafka accepts
  it, so the consumer is the first place it is detectable.

## Configuration

```jsonc
{
  "Messaging": { "Transport": "Kafka" },       // or "ServiceBus" (default), or "KafkaFlow"
  "Kafka": {
    "BootstrapServers": "broker-1:9092,broker-2:9092",
    "Topic": "attributes-events",
    "ConsumerGroup": "anishop-attributes-queries",
    "MaxConcurrentPartitions": 32,
    "MaxConcurrentSessions": 1000,
    "MaxMessagesPerSession": 100,
    "PartitionBufferSize": 4000,
    "RetryBackoffMilliseconds": 200,
    "MaxRetryBackoffMilliseconds": 30000
  }
}
```

`BootstrapServers`, `Topic` and `ConsumerGroup` are `[Required]`. As with the Service Bus options,
validation runs when the options are first resolved (processor construction at startup) rather than
via `ValidateOnStart`, so test hosts that strip the listener still boot against the empty
`appsettings.json` placeholders. The full option set is documented in the package README.

## Where the code lives

| File | Role |
|---|---|
| **Package** — `AnisShop.Kafka.Sessions` | |
| `KafkaSessionProcessor.cs` | Owns the single consumer: polls, routes, pauses/resumes, stores positions, handles rebalances. Exposes `ProcessSessionMessagesAsync` / `ProcessErrorAsync` and `StartProcessingAsync` / `StopProcessingAsync`, like `ServiceBusSessionProcessor` |
| `PartitionSessionWorker.cs` | One per partition: batches, groups by key, fans out, blocks on failure, computes the cursor |
| `OffsetWatermark.cs` | The cursor rule, isolated so it can be tested on its own |
| `SessionEventArgs.cs` | The two event-arg types |
| **This application** | |
| `Infrastructure/Kafka/KafkaEventListener.cs` | Subscribes, starts and stops the processor — the same shape as `ServiceBusEventListener`. Owns everything version-related: deserialize, project, throw on an unknown type or a gap |
| `Infrastructure/Kafka/KafkaEventDeserializer.cs` | Reads the `type` header; shares the event type map with the Service Bus reader |
| `Infrastructure/Kafka/KafkaRegisterExtension.cs` | Processor + deserializer + hosted service |
| `Infrastructure/Messaging/EventPayloadDeserializer.cs` | Shared with Service Bus: the event type map and JSON contract |
| `Infrastructure/Messaging/EventTransportRegisterExtension.cs` | The `Messaging:Transport` switch, now three-way |
| `Infrastructure/ServiceBus/EventBatchProcessor.cs` | Service Bus's own version/gap rules — the Kafka path does the equivalent inline in `KafkaEventListener` |

### The whole consuming side

```csharp
public async Task ProcessSessionAsync(ProcessSessionMessagesEventArgs args)
{
    var events = new List<EventBase>(args.Messages.Count);

    foreach (var message in args.Messages)
    {
        var @event = _deserializer.Deserialize(message)
            ?? throw new InvalidOperationException($"Cannot deserialize {args.Partition}@{message.Offset.Value}.");

        events.Add(@event);
    }

    using var scope = _scopeFactory.CreateScope();
    var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

    if (!await mediator.Send(new IncomingEvents { Events = events }, args.CancellationToken))
        throw new InvalidOperationException($"Version gap for aggregate {args.SessionId}.");
}
```

## Tests

All broker-free — `ConsumeResult<string, byte[]>` is a plain object, so a partition can be faked by
handing out sequential offsets in append order.

**Package suite** (`test/AnisShop.Kafka.Sessions.Tests`, 21) — no application, no EF, no host.

| Test | Proves |
|---|---|
| `Handle_InterleavedSessions_DeliversEachSessionInSenderOrder` | Three sessions interleaved in one partition each come back in sender order |
| `Handle_ManySessionsInOneBatch_DeliversAllOfThem` | 50 sessions in a single batch |
| `Handle_SessionsInOneBatch_RunConcurrently` | The fan-out is real: five calls block until all five are in flight, so a serialised implementation could never finish |
| `Handle_OneSession_NeverOverlapsWithItself` | The other half of the session guarantee — parallelism is between sessions only |
| `Handle_SessionLargerThanMaxMessagesPerSession_DeliversInSeveralOrderedCalls` | Chunking at the cap, in order, remainder last |
| `Handle_MessagesWithNoKey_AreDeliveredAsOneSession` | An unkeyed message is neither discarded nor silently parallelised |
| `Handle_HandlerThrows_BlocksThePartitionAndStopsConsuming` | A throw stops the partition — a good message offered behind it is never consumed |
| `Handle_HandlerRecovers_DrainsTheBacklogInOrder` | Blocking unblocks itself with the backlog intact |
| `Handle_HandlerThrows_RaisesTheErrorEvent` | Failures are observable, not just logged |
| `Handle_FullBuffer_StopsAcceptingRecords` | Backpressure: the buffer refuses records, which is what pauses the partition on the broker |
| `Handle_RedeliveredMessages_AreHandedOverAgain` | No transport-level deduplication — pinned so nobody assumes otherwise |
| `OffsetWatermarkTests` | The cursor never passes an unfinished message and never rewinds |
| `KafkaSessionProcessorOptionsTests` | `MaxConcurrentSessions >= MaxConcurrentPartitions`, and the required connection details |

**Application suite** (`test/AnisShop.Attributes.Queries.Tests/Kafka`, 8) — only what is ours.

| Test | Proves |
|---|---|
| `KafkaEventDeserializerTests` | Both type-header spellings, and null for anything unreadable |
| `KafkaProjectionWiringTests` | Our handler, driven through a real partition worker, lands interleaved sessions in the read model in publish order; an unknown event type blocks rather than skipping; a version gap blocks because the publisher's promise was violated |

The transport switch itself is covered once for all three, in
`test/AnisShop.Attributes.Queries.Tests/Messaging/EventTransportRegistrationTests` (6): exactly one
listener is registered, and the default is still Service Bus.
