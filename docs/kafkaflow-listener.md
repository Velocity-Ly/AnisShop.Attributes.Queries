# KafkaFlow Listener — the same job, bought off the shelf

> **Purpose**: A third value of `Messaging:Transport` that reads the same topic, in the same
> envelope, into the same `IncomingEvents` projection — but hands the consumer, the worker pool, the
> buffering and the offset management to [KafkaFlow](https://github.com/Farfetch/kafkaflow) instead
> of to `AnisShop.Kafka.Sessions`.
>
> It exists to answer one question: **how much of what we wrote already existed?** Read
> [`kafka-listener.md`](kafka-listener.md) first — it describes the transport this one is being
> measured against.

---

## The whole listener

Everything the application contributes:

```csharp
public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
{
    await _projector.ProjectAsync(context.GetMessagesBatch(), context.ConsumerContext.WorkerStopped);

    await next(context);
}
```

…plus a projector that regroups the batch and calls the same mediator handler the other two
transports call:

```csharp
foreach (var aggregate in batch.GroupBy(AggregateIdOf))
    await ProjectAggregateAsync(aggregate.Key, [.. aggregate], cancellationToken);
```

…and a registration:

```csharp
services.AddKafkaFlowHostedService(kafka => kafka
    .UseMicrosoftLog()
    .AddCluster(cluster => cluster
        .WithBrokers(options.BootstrapServers.Split(','))
        .AddConsumer(consumer => consumer
            .Topic(options.Topic)
            .WithGroupId(options.ConsumerGroup)
            .WithWorkersCount(options.WorkersCount)
            .WithBufferSize(options.BufferSize)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithWorkerDistributionStrategy<BytesSumDistributionStrategy>()
            .AddMiddlewares(middlewares => middlewares
                .AddBatching(options.BatchSize, TimeSpan.FromMilliseconds(options.BatchTimeoutMs))
                .Add<EventProjectionMiddleware>()))));
```

There is **no hosted service, no consume loop, no partition worker, no buffer, no pause/resume, no
rebalance handling and no offset arithmetic** in this transport. All of it comes out of the package.

## How KafkaFlow gets ordering and parallelism

Two mechanisms, and it is worth being precise about both because they are *not* the ones
`AnisShop.Kafka.Sessions` uses.

**Parallelism is a fixed pool of workers**, sized by `WithWorkersCount`. Workers are not tied to
partitions — a worker can receive messages from any partition the consumer owns.

**Ordering comes from how a message picks its worker.** `BytesSumDistributionStrategy` sums the bytes
of the message key and takes it modulo the worker count:

```csharp
for (var i = 0; i < context.RawMessageKey.Value.Length; i++)
    bytesSum += context.RawMessageKey.Value.Span[i];

return _workers.ElementAtOrDefault(bytesSum % _workers.Count);
```

The same key therefore always lands on the same worker, and a worker processes its messages one at a
time — so one aggregate is ordered, and different aggregates run concurrently. The partition is
irrelevant to the guarantee, which is a genuinely nice property: repartitioning does not change
anything about ordering.

`AddBatching` then collects up to `BatchSize` messages per worker (or waits `BatchTimeout`) and hands
them over in one call. That is what makes the projector's `GroupBy` necessary: **a worker's batch is
a hash bucket, not an aggregate.** It holds every key that hashed to this worker, possibly from
several partitions, in arrival order.

```
worker 3 batch:  A1 F1 A2 F2 A3          ← two aggregates that hashed alike, plus whatever else
                        │
                  group by message key
                        │
              ┌─────────┴─────────┐
           A1 A2 A3            F1 F2      ← projected one after another, in this worker
```

## What it does the same way we did

Two things stand out, and they are the two hardest parts of the problem:

**The offset watermark.** KafkaFlow runs `EnableAutoCommit = true` with `EnableAutoOffsetStore =
false` — the identical configuration `KafkaSessionProcessor` uses — and its `PartitionOffsets` class
only advances the committable offset when the *oldest* received message has been processed, then
drains the contiguous run behind it:

```csharp
if (context.Offset != _receivedContexts.First.Value.Offset)
{
    _processedContexts.Add(context.Offset, context);
    return false;                       // something older is still in flight; commit stays put
}
```

That is `OffsetWatermark.TryAdvance` with a different data structure. Independently arriving at the
same rule is decent evidence the rule is right — and equally good evidence we did not need to write
it.

**Backpressure.** `WithBufferSize` is a per-worker bound, the counterpart of `PartitionBufferSize`.

## What it does differently — the four that matter

### 1. Concurrency is capped by worker count, not by aggregate count

`AnisShop.Kafka.Sessions` fans out **every distinct key in a batch**, bounded by
`MaxConcurrentSessions` (1000). KafkaFlow fans out across `WorkersCount` workers, and two unrelated
aggregates that hash to the same worker **wait for each other** — head-of-line blocking between
aggregates that have nothing to do with each other.

With 32 workers and 1000 active aggregates, roughly 31 aggregates share each worker. To match a
1000-way fan-out you would set `WorkersCount = 1000`, which is 1000 long-lived tasks and 1000
buffers rather than 1000 semaphore slots.

Whether this matters depends entirely on where the bottleneck actually is. If the read-model
database saturates at 30 concurrent writers, 32 workers is already past the useful limit and this
costs nothing.

### 2. A failed batch is skipped, not retried

This is the big one, and it is not a configuration detail — it is KafkaFlow's model.

When the middleware throws, `BatchConsumeMiddleware` catches it, writes one line through the log
handler, and then completes **every message in the batch** anyway:

```csharp
catch (Exception ex)
{
    _logHandler.Error("Error executing a message batch", ex, new { ... });
}
finally
{
    _batch.Clear();
    _dispatchSemaphore.Release();

    if (_consumerConfiguration.AutoMessageCompletion)
    {
        foreach (var messageContext in localBatch)
            messageContext.ConsumerContext.Complete();
    }
}
```

Completed means the offset is stored and then committed. So a poison message does not block — it is
**dropped**, along with every other aggregate that happened to be in that worker's batch. Up to
`BatchSize` messages, across many unrelated aggregates and possibly several partitions, leave a
permanent hole in the read model.

The hand-rolled transport does the exact opposite: it blocks the partition forever and never
advances the cursor, on the reasoning that a hole nothing downstream can detect is worse than lag
somebody gets paged for.

Because the exception never escapes the batching middleware, the `MessageConsumeError` global event
does not fire either — the only signal is that one log line. That is why
`KafkaFlowEventProjector.Fail` logs `Critical` with the aggregate id, partition and every offset
before it throws: it is the only record of what was lost.

**Three ways to close this, none of them free:**

| Option | Cost |
|---|---|
| `KafkaFlow.Retry` with a retry-forever policy | An extra package, currently at 3.1.0 against a 4.2.0 core |
| Drop `AddBatching` | One database round trip per event, and the blast radius shrinks to one message rather than disappearing |
| `WithManualMessageCompletion()` and only `Complete()` on success | Uncommitted offsets replay on restart, but `PartitionOffsets.WaitContextsCompletionAsync` waits on every received-and-uncompleted context, so a permanently uncompleted message hangs the next rebalance or shutdown |

None of these were applied here. This transport is deliberately idiomatic KafkaFlow so the comparison
is honest.

### 3. Failure blast radius is a worker batch, not a partition

Related but distinct. Our blocked partition stops one partition and nothing else; KafkaFlow's failed
batch discards a slice of *whatever the worker was holding*, which can span partitions. Neither is
strictly smaller — they fail in different shapes.

### 4. One handler call, one aggregate — but only after we regroup

`ProcessSessionMessagesAsync` delivers a run of one session's messages, ready to project.
KafkaFlow's batch has to be regrouped first. That is nine lines, not an architectural difference,
but it is the reason the projector exists at all rather than the middleware calling mediator
directly.

## Code, weighed

| Transport | Application code | Package code we maintain |
|---|---|---|
| Service Bus | 381 lines | — (Azure SDK) |
| Kafka (`AnisShop.Kafka.Sessions`) | 141 lines | **1030 lines** |
| KafkaFlow | 260 lines | — (Farfetch) |

The KafkaFlow column is larger than the hand-rolled application column because the batch regrouping
and options live there rather than in a package. The 1030-line column is the one that disappears.

## Configuration

```jsonc
{
  "Messaging": { "Transport": "KafkaFlow" },
  "KafkaFlow": {
    "BootstrapServers": "broker-1:9092,broker-2:9092",
    "Topic": "attributes-events",
    "ConsumerGroup": "anishop-attributes-queries",
    "WorkersCount": 32,                  // the only parallelism knob
    "BufferSize": 100,                   // per worker
    "BatchSize": 100,
    "BatchTimeoutMilliseconds": 25
  }
}
```

`BootstrapServers`, `Topic` and `ConsumerGroup` are `[Required]`, and — unlike the other two
listeners — **validation runs at registration**. KafkaFlow builds its entire topology while
`AddKafka` runs, so the values are needed then rather than when something first resolves `IOptions`.
That is safe here only because nothing registers this transport unless `Messaging:Transport` names
it, so the empty `appsettings.json` placeholders never reach it.

The namespace is `Infrastructure.KafkaFlowTransport`, not `Infrastructure.KafkaFlow`, because the
package owns the `KafkaFlow` root namespace and a folder of the same name shadows it in every file.

## Where the code lives

| File | Role |
|---|---|
| `Infrastructure/KafkaFlowTransport/KafkaFlowRegisterExtension.cs` | The topology: brokers, workers, distribution strategy, batching, middleware |
| `Infrastructure/KafkaFlowTransport/EventProjectionMiddleware.cs` | Pulls the batch out of the context; nothing else |
| `Infrastructure/KafkaFlowTransport/KafkaFlowEventProjector.cs` | Regroups by key, deserializes, projects, and logs precisely what a throw is about to lose |
| `Infrastructure/KafkaFlowTransport/KafkaFlowEventDeserializer.cs` | Reads the `type` header off `IMessageHeaders`; shares the event type map with both other transports |
| `Infrastructure/KafkaFlowTransport/KafkaFlowListenerOptions.cs` | The `KafkaFlow` configuration section |

## Tests

`test/AnisShop.Attributes.Queries.Tests/KafkaFlowTransport` (9), plus 2 added to
`Messaging/EventTransportRegistrationTests`.

The suite is far smaller than the Kafka one, and that is the point: session grouping, ordering,
blocking, backpressure and cursor arithmetic are the package's to prove, not ours. There is also no
harness and no waiting — the projector is called directly, because there is no loop of ours to run.

| Test | Proves |
|---|---|
| `Project_TwoAggregatesInOneBatch_ReachTheReadModelInPublishOrder` | A hash bucket carrying two interleaved aggregates is split back into ordered runs |
| `Project_ReplayedBatch_LeavesTheReadModelUnchanged` | At-least-once is absorbed by the projection — nothing here deduplicates |
| `Project_UnknownEventType_ThrowsAndAbandonsTheRestOfTheBatch` | The blast radius, pinned as a test rather than left as a claim |
| `Project_VersionGap_ThrowsBecauseThePublisherOrderWasViolated` | A gap is a broken publisher promise, and it is loud |
| `KafkaFlowEventDeserializerTests` (5) | Both type-header spellings, null for anything unreadable |
| `EventTransportRegistrationTests` (+2) | Only the package's hosted service is registered, and our middleware and projector alongside it |

## Verdict, and what is still unknown

KafkaFlow does the job, in a quarter of the code, with production mileage on exactly the parts we
cannot test here — rebalance, commit races, pause/resume. Its offset manager solves the same problem
the same way ours does.

Against that: the failure policy is not ours and cannot be made ours without giving something up,
and parallelism is capped by worker count with head-of-line blocking between unrelated aggregates.

**Neither Kafka transport has ever run against a real broker.** Until both do — same topic, same
load, and a rebalance forced mid-flight — this comparison is a reading of two codebases, not a
measurement. That is the next thing to do, and it needs doing whichever one ships.
