# Getting Started

If you have used `ServiceBusSessionProcessor`, you already know this API. Subscribe to a handler,
start the processor, and messages of one session arrive in sender order while different sessions run
in parallel. The only difference in the handler signature is that you get **a run of messages**
rather than one.

The deal is symmetric, and it is the same deal sessions offer:

| You guarantee | The package guarantees |
|---|---|
| The sender sets a message key on every record | One session never spans two partitions |
| The sender produces a session's messages in the order you want them applied | Your handler receives them in exactly that order |
| Your handler is idempotent | Two calls for the same session are never in flight at once |
| — | Offsets only ever commit below the oldest message you have not finished |

The broker cannot check that the sender segmented or ordered correctly, and neither can this — same
as `SessionId`. Everything below assumes the sender held up its end.

---

## Step 1 — Reference the package

```xml
<ProjectReference Include="..\AnisShop.Kafka.Sessions\AnisShop.Kafka.Sessions.csproj" />
```

```csharp
services.AddKafkaSessionProcessor(builder.Configuration);
```

That binds the options and registers a `KafkaSessionProcessor` singleton. Starting it is your job —
the same small hosted service you would write for a Service Bus processor.

## Step 2 — Write the listener

```csharp
public class OrderEventListener : IHostedService
{
    private readonly KafkaSessionProcessor _processor;
    private readonly IServiceScopeFactory _scopeFactory;

    public OrderEventListener(KafkaSessionProcessor processor, IServiceScopeFactory scopeFactory)
    {
        _processor = processor;
        _scopeFactory = scopeFactory;

        _processor.ProcessSessionMessagesAsync += ProcessSessionAsync;
        _processor.ProcessErrorAsync += OnErrorAsync;
    }

    public Task StartAsync(CancellationToken ct) => _processor.StartProcessingAsync(ct);

    public Task StopAsync(CancellationToken ct) => _processor.StopProcessingAsync(ct);

    private async Task ProcessSessionAsync(ProcessSessionMessagesEventArgs args)
    {
        // args.SessionId is the key the sender set — your order id, aggregate id, whatever.
        // args.Messages are that session's messages, in the order the sender produced them.
        var events = args.Messages.Select(Deserialize).ToList();

        using var scope = _scopeFactory.CreateScope();
        var readModel = scope.ServiceProvider.GetRequiredService<IOrderReadModel>();

        await readModel.ApplyAsync(args.SessionId, events, args.CancellationToken);
    }

    private Task OnErrorAsync(ProcessSessionErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Session {SessionId} on {Partition} failed",
            args.SessionId, args.Partition);

        return Task.CompletedTask;
    }
}
```

```csharp
services.AddHostedService<OrderEventListener>();
```

**Return means done.** The offsets move past those messages.

**Throw means not done.** The partition blocks, the same messages come back on the next attempt, and
nothing is discarded. That is the entire failure model — there is no "abandon", no "dead letter", no
return value to get wrong.

**Scope it yourself.** Exactly as with a Service Bus handler: create a DI scope inside the handler if
you need scoped services. Sessions run in parallel, so a `DbContext` must not be shared.

## Step 3 — Be idempotent

The cursor can only sit below the oldest unfinished message, so a restart or rebalance re-reads a
tail you have already handled. The package does not deduplicate — it cannot, because it never looks
inside your payload.

This is where your sequence numbers live, if you have them:

```csharp
var current = order?.Version ?? 0;

foreach (var @event in events.Where(@event => @event.Version > current))
{
    Apply(order, @event);
    current = @event.Version;
}
```

Note what that code is *not* doing: it does not sort, and it does not check for gaps. Under the
ordering the sender promised, version N-1 was handled before version N arrived. If you want to
detect a broken promise, do it here and throw — the partition will block loudly, which is the right
response to a violated invariant.

## Step 4 — Configure

```jsonc
{
  "Kafka": {
    "BootstrapServers": "broker-1:9092,broker-2:9092",
    "Topic": "order-events",
    "ConsumerGroup": "orders-read-model",

    "MaxConcurrentPartitions": 32,
    "MaxConcurrentSessions": 1000,
    "MaxMessagesPerSession": 100
  }
}
```

**The three that matter:**

- **`MaxConcurrentSessions`** — the direct counterpart of `ServiceBusSessionProcessor`'s. Size it
  against your database connection pool, not your partition count; this is the knob that exhausts a
  pool. It must be at least `MaxConcurrentPartitions`, and startup will tell you if it is not.
- **`MaxConcurrentPartitions`** — how many partitions may be handling a batch at once. Your ceiling
  on in-flight work, and therefore on memory.
- **`MaxMessagesPerSession`** — how many of one session's messages arrive per call. Bigger means
  fewer, fatter calls.

For SASL, SSL or timeout settings, construct the processor with the `configureConsumer` hook rather
than through configuration:

```csharp
new KafkaSessionProcessor(options, loggerFactory, config =>
{
    config.SecurityProtocol = SecurityProtocol.SaslSsl;
    config.SaslMechanism = SaslMechanism.ScramSha512;
    config.SaslUsername = username;
    config.SaslPassword = password;
});
```

## Step 5 — Keep it out of your tests

Remove the hosted service that starts the processor; the processor registration itself is harmless
because it opens no consumer until `StartProcessingAsync`. Your tests then call your handler
directly, which is the same code path the processor drives.

---

## Operating it

**Scale out** by adding instances to the consumer group; each gets a disjoint slice of the
partitions. Your ceiling is the partition count.

**Alert on exactly two things:**

1. **Consumer lag** on any partition.
2. **The `Critical` log line** — `"{Partition} is blocked at offset {Offset}"`, emitted once when a
   partition stops, then `Warning` per retry, then `Information` on recovery.

A blocked partition is the accepted cost of never discarding a message. Most blockages resolve
themselves — a database returning, or a deploy that teaches your handler a new message type, since
the bytes are still sitting in the partition.

## Checklist before you ship

- [ ] The sender sets a key on **every** message — an unkeyed message has no session and no ordering
- [ ] The sender produces each session's messages in the order you want them applied
- [ ] Your handler is idempotent
- [ ] Your handler throws rather than swallowing what it cannot process
- [ ] `MaxConcurrentSessions` fits your database connection pool
- [ ] Alerts on consumer lag and on the `Critical` blocked-partition line
- [ ] Partitions provisioned generously — adding them later re-hashes keys and breaks ordering across
      the change

**One known limitation:** `AddKafkaSessionProcessor` binds a single unnamed options instance, so
calling it twice for two topics makes both share one configuration. For a second topic in the same
process, construct a `KafkaSessionProcessor` directly with its own options object.
