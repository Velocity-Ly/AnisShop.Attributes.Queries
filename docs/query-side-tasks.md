# Query Side Implementation Tasks

> **Purpose**: Track the tasks required to fully build out the Queries event consumer and projections.
> Each task is designed to be completed in a single conversation session.
> Update the status after completing each task.

---

## Task 1 — Event DTOs
**Status**: DONE

Define all 15 event deserialization records in the Queries project. Simple POCOs matching the camelCase JSON shape published by Commands. Include an event type name constant for each (the discriminator string from the Service Bus message `Type` property).

**Events to define**:
AttributeCreated, AttributePublished, AttributeOptionAdded, AttributeApplicableCategoriesAdded, AttributeMetadataChanged, AttributeDeleted, AttributeMarkedAsDeprecated, AttributeDeprecationWarningRemoved, AttributeDisabled, AttributeOptionRemoved, AttributeOptionDisabled, AttributeOptionLabelChanged, AttributeOptionsReordered, AttributeTypeChanged, AttributeApplicableCategoriesRemoved

**Key decisions**:
- Queries defines its own DTOs (does NOT reference Commands project)
- JSON shape: camelCase naming, nulls ignored, System.Text.Json
- Each DTO needs the base fields: AggregateId (Guid), Version (int), UserId (string), DateTime (DateTime)
- Plus event-specific Data payload

---

## Task 2 — Read Model Schema Update
**Status**: NOT STARTED

Add DeprecationWarning and DisableReason to the Attribute entity:
- `ArabicDeprecationWarning` (string?, MaxLength 1000)
- `EnglishDeprecationWarning` (string?, MaxLength 1000)
- `ArabicDisableReason` (string?, MaxLength 1000)
- `EnglishDisableReason` (string?, MaxLength 1000)

Update:
- `Domain/Attribute.cs` — add properties
- `Infrastructure/Persistence/Configurations/AttributeConfigurations.cs` — add column config
- Generate EF Core migration

---

## Task 3 — Event Consumer Infrastructure
**Status**: DONE

The core consumer that listens to Azure Service Bus and routes events to projectors.

**Implemented components** (`Infrastructure/ServiceBus/`):

| File | Purpose |
|---|---|
| `ServiceBusListenerOptions.cs` | Config POCO — topic, subscription, max concurrent sessions (1000), max messages per session (100), DLQ toggle |
| `IEventDeserializer.cs` / `EventDeserializer.cs` | Maps `EventTypeNames` → concrete types via `FrozenDictionary`, deserializes JSON body from Service Bus messages |
| `EventBatchProcessor.cs` | Pure logic: sort by version, deduplicate, detect version gaps, return contiguous prefix. Independently testable (no Service Bus coupling) |
| `ServiceBusEventListener.cs` | `IHostedService` — manual session loop + dead letter queue processor |

**Architecture decisions made**:
- **Manual session management** instead of `ServiceBusSessionProcessor` — the built-in processor delivers one message at a time, we need batches of 100 per session
- Uses `AcceptNextSessionAsync` + `ReceiveMessagesAsync(maxMessages: 100)` for true batch behavior
- `SemaphoreSlim(1000)` caps concurrent sessions as a safety ceiling (not a lock — prevents resource exhaustion during bursts)
- **Two concurrent loops**: main session loop (fire-and-forget per session) + DLQ processor (`ServiceBusProcessor` with `SubQueue.DeadLetter`)
- **Gap detection**: versions `[4,5,6,8,9,10]` → sends `[4,5,6]` to handler, stops at gap. Only checks `Version == prev + 1`, does NOT validate starting version (handler's idempotency check handles that)
- **Duplicate handling**: same-version messages are completed (removed) at the Service Bus level before reaching the handler
- **DLQ events**: sent individually (single event in a list) to the same `IncomingEvents` handler
- `EventBatchProcessor` is a standalone class (not a Mediator pipeline behavior) — simpler, more explicit, easier to test
- Handler returns `bool`: `true` → complete contiguous messages, `false` → don't complete (lock expires, messages retry)

**DI registrations** added to `Program.cs`:
- `ServiceBusClient` (singleton, from connection string)
- `ServiceBusListenerOptions` (bound from `ServiceBus` config section)
- `IEventDeserializer` / `EventDeserializer` (singleton)
- `EventBatchProcessor` (singleton)
- `ServiceBusEventListener` (hosted service)

**Config** added to `appsettings.json`:
- `ConnectionStrings:ServiceBus` — Azure Service Bus connection string
- `ServiceBus:TopicName`, `ServiceBus:SubscriptionName`

**Still NOT implemented** (out of scope for this task):
- `IncomingEventsHandler` body (currently returns `true`) — see Tasks 4 & 5
- Polly retry wrapping — to be added when projectors are implemented
- Idempotency enforcement (`event.Version == currentAttribute.Version + 1`) — lives in the handler, not the listener

---

## Task 4 — Attribute & Category Projectors
**Status**: NOT STARTED

Projector methods for attribute-level and category events:

| Event | Read Model Mutation |
|---|---|
| `AttributeCreated` | INSERT Attribute (Status=Draft, Version=1) |
| `AttributePublished` | UPDATE Status=Published |
| `AttributeMetadataChanged` | UPDATE display names & descriptions |
| `AttributeTypeChanged` | UPDATE Type |
| `AttributeMarkedAsDeprecated` | UPDATE Status=Deprecated + set DeprecationWarning |
| `AttributeDeprecationWarningRemoved` | UPDATE Status=Published + clear DeprecationWarning |
| `AttributeDisabled` | UPDATE Status=Disabled + set DisableReason |
| `AttributeDeleted` | DELETE Attribute (cascade deletes options & categories) |
| `AttributeApplicableCategoriesAdded` | INSERT AttributeCategory rows |
| `AttributeApplicableCategoriesRemoved` | DELETE AttributeCategory rows |

**Important**: Every event must UPDATE `Attribute.Version` to the event's Version.

---

## Task 5 — Option Projectors
**Status**: NOT STARTED

| Event | Read Model Mutation |
|---|---|
| `AttributeOptionAdded` | INSERT AttributeOption (SortOrder = MAX(existing) + 1) |
| `AttributeOptionLabelChanged` | UPDATE ArabicLabel, EnglishLabel |
| `AttributeOptionDisabled` | UPDATE IsDisabled = true |
| `AttributeOptionRemoved` | DELETE AttributeOption row |
| `AttributeOptionsReordered` | UPDATE SortOrder for each key based on array index |

**Important**: Every event must also UPDATE `Attribute.Version`.

---

## Task 6 — Proto & gRPC Updates
**Status**: NOT STARTED

Expose DeprecationWarning and DisableReason through gRPC:
- Update `AttributeOutput` proto message with new fields
- Update `GetAttributeResult` / `GetByCategoryResult` to include new fields
- Update mapping extensions in `QueriesExtensions.cs`
- Update query handlers if needed
- Update test proto file in test project
- Update assert helpers and fakers if needed

---

## Task 7 — Unit Tests for Event Consumer
**Status**: NOT STARTED

Using WebApplicationFactory + EF Core InMemory:
- Test each projector independently (happy path)
- Test idempotency (replaying same Version → skip)
- Test out-of-order (Version gap → reject/skip)
- Test unknown event type → handled gracefully
- Test AttributeCreated when attribute already exists → idempotent skip
- Test cascade delete via AttributeDeleted
- Create EventFaker(s) with Bogus for test data generation

---

## Task 8 — Integration Tests
**Status**: NOT STARTED

Using LocalDB + real SQL Server:
- Full event sequence: Create → AddOptions → AddCategories → Publish → query and verify
- Full lifecycle: Create → Publish → Deprecate → RemoveDeprecation → Disable → Delete
- Verify concurrency token (Version) behavior with real DB
- Verify cascade deletes
- Verify SortOrder management through add/reorder/remove sequences
