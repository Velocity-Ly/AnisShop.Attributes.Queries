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
**Status**: DONE

Add DeprecationWarning and DisableReason to the Attribute entity:
- `ArabicDeprecationWarning` (string?, MaxLength 1000)
- `EnglishDeprecationWarning` (string?, MaxLength 1000)
- `ArabicDisableReason` (string?, MaxLength 1000)
- `EnglishDisableReason` (string?, MaxLength 1000)

Done:
- `Domain/Attribute.cs` — added 4 nullable properties (private setters, after `Status`)
- `Infrastructure/Persistence/Configurations/AttributeConfigurations.cs` — added `HasMaxLength(1000)` for each
- Generated EF Core migration `Migrations/20260523103612_InitialCreate.cs`

**Note**: No migration existed before this task, so the generated `InitialCreate` is the schema baseline — it creates all three tables (Attributes, AttributeOptions, AttributeCategories) **including** the new columns. This also fixes the production `DatabaseRunner.MigrateAsync()` path, which previously had nothing to apply. Tests are unaffected: unit tests use EF InMemory, integration tests use `EnsureCreatedAsync()` (model-driven, not migration-driven).

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
- ~~Polly retry wrapping — to be added when projectors are implemented~~ → DONE, see **Task 5.1 — Projector Hardening**
- Idempotency enforcement (`event.Version == currentAttribute.Version + 1`) — lives in the handler, not the listener

---

## Task 4 — Attribute & Category Projectors
**Status**: DONE

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

**Implementation**:
- `Domain/Attribute.cs` — added a `static Create(...)` factory (Status=Draft) plus encapsulated projection methods (`Publish`, `ChangeMetadata`, `ChangeType`, `MarkAsDeprecated`, `RemoveDeprecationWarning`, `Disable`, `AddCategories`, `RemoveCategories`). Each mutator sets `Version` to the event's version. Private setters stay private — the aggregate owns its mutations.
- `Domain/AttributeCategory.cs` — added `internal static Create(...)` so the `Attribute` aggregate can materialise child category rows (`AddCategories` skips duplicates; `RemoveCategories` removes by id set). Collections are mutable `ICollection`, so add/remove needs no setter access.
- `Features/EventsHandler/IncomingEventsHandler.cs` — replaced the stub. Now: orders the batch by `Version`, loads the aggregate (`.Include` Options + ApplicableCategories), then per event: **skip** if `Version <= currentVersion` (idempotent replay), **return false** on a contiguity gap (`Version != currentVersion + 1`) so Service Bus redelivers, otherwise dispatch via a type switch and advance `currentVersion`. One `SaveChangesAsync` per batch = single transaction. `AttributeDeleted` removes the row (EF cascades children); the 5 option events throw `NotImplementedException` pending Task 5.

**Verified**: 39/42 unit tests green. The 3 reds are the option-dependent tests (`Handle_OptionLifecycle`, `Handle_MixedBatch`, `Handle_PartialReplay`) — they belong to Task 5. All attribute/category coverage passes: full lifecycle (Create→Publish→MetadataChanged→Deprecate→RemoveDeprecation→Disable→Delete), category add/remove lifecycle, fresh/full-replay idempotency, version-already-processed, and version-gap rejection.

---

## Task 5 — Option Projectors
**Status**: DONE

| Event | Read Model Mutation |
|---|---|
| `AttributeOptionAdded` | INSERT AttributeOption (SortOrder = MAX(existing) + 1) |
| `AttributeOptionLabelChanged` | UPDATE ArabicLabel, EnglishLabel |
| `AttributeOptionDisabled` | UPDATE IsDisabled = true |
| `AttributeOptionRemoved` | DELETE AttributeOption row |
| `AttributeOptionsReordered` | UPDATE SortOrder for each key based on array index |

**Important**: Every event must also UPDATE `Attribute.Version`.

**Implementation**:
- `Domain/AttributeOption.cs` — added an `internal static Create(...)` factory (always `IsDisabled=false`) plus `internal` mutators `ChangeLabel`, `Disable`, `SetSortOrder`. `internal` (not `public`) because only the `Attribute` aggregate drives option mutations. Private setters stay private — the option owns its own field writes.
- `Domain/Attribute.cs` — added 5 projection methods, each setting `Version` to the event version: `AddOption` (skips duplicate keys; SortOrder = `Options.Count == 0 ? 0 : Max+1`, i.e. append to bottom), `ChangeOptionLabel`, `DisableOption`, `RemoveOption` (no-op if key absent — all four tolerate a missing key for replay safety), `ReorderOptions` (array index = 0-based SortOrder per key).
- `Features/EventsHandler/IncomingEventsHandler.cs` — replaced the 5 `NotImplementedException` cases with calls to the new aggregate methods. Same batch flow as Task 4 (order → skip-already-processed → gap-reject → switch dispatch → single `SaveChangesAsync`). Options are loaded via the existing `.Include(a => a.Options)`.

**Verified**: 42/42 unit tests green (was 39/42). The 3 formerly-red option tests now pass — `EventSequenceProjectionTests.Handle_OptionLifecycle` (add 3 → reorder → label-change → disable → remove, final 2 options at V8), `Handle_MixedBatch` (option in a mixed batch), and `EventHandlerIdempotencyTests.Handle_PartialReplay` (option event among V4–V5 applied over a seeded V3). No regressions across the rest of the suite.

---

## Task 5.1 — Projector Hardening
**Status**: DONE

Deferred from Task 3: wrap the projector writes in Polly retry and enforce an explicit per-batch transaction boundary in `IncomingEventsHandler`.

**Implementation** (`Features/EventsHandler/IncomingEventsHandler.cs`):
- The load + `Apply` pass still runs ONCE. Only persistence moved into a new `PersistBatchAsync` so a retry never re-queries/re-tracks (which would duplicate inserts).
- **Explicit transaction**: `PersistBatchAsync` opens `_dbContext.BeginTransactionAsync` (the existing virtual method — `ReadCommitted` on SqlServer), saves, commits. The whole batch is one atomic unit, mirroring the Commands `SqlEventStore` pattern.
- **Commit-safe save**: `SaveChangesAsync(acceptAllChangesOnSuccess: false)` then `ChangeTracker.AcceptAllChanges()` AFTER `CommitAsync`. The tracker stays dirty until the transaction truly commits, so a transient Commit failure on retry re-sends the same changes instead of silently committing an empty batch (no lost projection).
- **Polly retry**: a shared `static readonly ResiliencePipeline` (`ProjectionPipeline`) — 3 attempts, 200 ms exponential backoff + jitter — wraps the transaction block. `ShouldHandle` matches only transient faults: `SqlException` where `IsTransient`, and `DbUpdateException` whose inner is a transient `SqlException`. `DbUpdateConcurrencyException` is deliberately NOT matched — it bubbles, the batch is left uncompleted, and Service Bus redelivers for a fresh idempotent re-projection.

**Test impact** (`test/.../FakeServices/FakeAttributesDbContext.cs`):
- The InMemory `FakeTransaction.Commit`/`CommitAsync` were made **no-ops** (previously re-called `SaveChanges`). InMemory has no real transaction and `SaveChanges(acceptAllChangesOnSuccess: false)` already wrote the rows while leaving entities Added — a re-`SaveChanges` in Commit would throw on duplicate keys. The handler's own `AcceptAllChanges()` resets the tracker. (Diverges intentionally from the Commands `FakeTransaction`, which still saves-on-commit because that handler saves with the default accept-on-success.)

**Verified**: `src` builds clean (0/0); full unit suite **42/42** green — no regressions. The retry pipeline never trips under InMemory (no transient faults), so behaviour is identical to before plus the atomic-commit guarantee.

---

## Task 6 — Proto & gRPC Updates
**Status**: DONE

Exposed DeprecationWarning and DisableReason through the whole query/gRPC stack. All four are nullable, so each is a `google.protobuf.StringValue` wrapper field — mirroring the existing `arabic_description`/`english_description` precedent (a null read-model string maps to an unset wrapper, i.e. a `null` C# property, round-tripping cleanly).

**Implementation**:
- `Protos/attributes_queries.proto` **and** `test/.../Proto/attributes_queries.proto` — added 4 fields to `AttributeOutput` (kept the two files identical apart from `csharp_namespace`): `arabic_deprecation_warning = 11`, `english_deprecation_warning = 12`, `arabic_disable_reason = 13`, `english_disable_reason = 14`. The `AttributeOptionOutput` message was untouched (the new fields live on the attribute, not the option).
- `Features/Queries/Get/GetAttributeResult.cs` (`GetAttributeResult`) and `Features/Queries/GetByCategory/GetByCategoryResult.cs` (`AttributeItem`) — added the 4 `string?` properties.
- `Features/Queries/Get/GetAttributeQueryHandler.cs` and `Features/Queries/GetByCategory/GetByCategoryQueryHandler.cs` — projected the 4 fields from the read-model `Attribute` (the existing query already loads the full row, so no extra `.Include`/column work was needed).
- `Extensions/QueriesExtensions.cs` — mapped the 4 fields in **both** `ToAttributeOutput()` overloads (`GetAttributeResult` and `AttributeItem`). Direct `string?` → `StringValue` assignment, same as the description fields.

**Test impact**:
- `test/.../Asserts/AssertEquality.cs` (`OfDomainAndResponse`) and `test/.../Asserts/AssertEquality.Queries.cs` (`OfDomainAndQueryResponse`) — added 4 equality assertions each, honouring the "target all properties" convention.
- `test/.../Fakers/Domain/AttributeFaker.cs` — added 4 `RuleFor`s populating the fields with Latin content (`f.Lorem.Sentence()`), so the existing query tests now exercise a **non-null** round-trip through the `StringValue` wrappers (proving the mapping, not just `null == null`). Honours the no-hardcoded-Arabic rule (Arabic-named fields get Latin Bogus content).

**Verified**: `src` builds clean (0/0); full unit suite **42/42** green — no regressions. The existing `Get_*`/`GetByCategory_*` gRPC tests now assert the new fields end-to-end via the updated `AttributeFaker` + assert helpers.

---

## Task 7 — Unit Tests for Event Consumer
**Status**: DONE

Filled the event-consumer coverage gaps left after Tasks 4/5. The existing suites
(`AttributeCreatedProjectionTests`, `AttributeMetadataChangedProjectionTests`,
`EventHandlerIdempotencyTests`, `EventSequenceProjectionTests`) already covered idempotency
(replay → skip), version-gap rejection, Created-already-exists idempotent skip, and the
full lifecycle/option/category sequences. This task added the missing per-projector and
deserializer coverage. The 15 Bogus `EventFaker`s already existed (Task 3), so no new fakers
were needed — the new tests reuse them via direct construction and the `EventHistoryBuilder`.

**Coverage map (which task's tests cover each bullet)**:

| Task 7 bullet | Covered by |
|---|---|
| Each projector independently (happy path) | **NEW** `ProjectorHappyPathTests` — Published, TypeChanged, MarkedAsDeprecated, DeprecationWarningRemoved, Disabled, CategoriesAdded, CategoriesRemoved, OptionAdded, OptionLabelChanged, OptionDisabled, OptionRemoved, OptionsReordered (+ Created/MetadataChanged already had dedicated tests) |
| Idempotency (replay same Version → skip) | `EventHandlerIdempotencyTests` + `AttributeMetadataChangedProjectionTests` |
| Out-of-order (Version gap → reject) | `EventHandlerIdempotencyTests.Handle_VersionGap` + `AttributeMetadataChangedProjectionTests.Handle_MetadataChanged_WhenVersionGapExists` |
| Unknown event type → handled gracefully | **NEW** `EventDeserializerTests` |
| AttributeCreated when already exists → skip | `AttributeCreatedProjectionTests.Handle_AttributeCreated_WhenAttributeAlreadyExists` |
| Cascade delete via AttributeDeleted | **NEW** `ProjectorHappyPathTests.Handle_Deleted_WithOptionsAndCategories_CascadeDeletesChildren` |
| EventFaker(s) with Bogus | Pre-existing (15 fakers under `test/.../Fakers/Events/`) |

**New files** (`test/.../EventsHandler/`):
- `ProjectorHappyPathTests.cs` — 13 isolated tests. Each seeds a known precondition and sends
  the **single** event under test so a failure points at one projector. Attribute-/category-level
  projectors seed via `DatabaseHelper.InsertAsync(AttributeFaker...)` at V1 then apply V2.
  Option-mutation projectors (label/disable/remove/reorder) need a **known** option key, so they
  seed via `EventHistoryBuilder.BuildUpTo(n)` (Created → OptionAdded("color-red") …) then send the
  mutation as a separate batch — `OptionAdded` always materialises `IsDisabled=false`, giving the
  disable test a meaningful flip. The cascade-delete test seeds an attribute carrying 3 options +
  2 categories, deletes it, then asserts `AttributeOptions`/`AttributeCategories` both have **zero**
  rows for the aggregate (EF InMemory cascades the loaded, tracked dependents).
- `EventDeserializerTests.cs` — 3 pure unit tests (no WebApplicationFactory; `NullLogger` +
  `ServiceBusModelFactory.ServiceBusReceivedMessage`): known type round-trips to the typed event
  (anchor, proving the null cases aren't false positives), **unknown subject → null** (the
  "handled gracefully" bullet), and **missing type → null**.

**No `src` changes** — this task is test-only. Honoured the no-hardcoded-Arabic rule: all
Arabic-named fields use Latin placeholders (`"Arabic Red"`, `"Arabic Deprecation Warning"`, …).

**Verified**: full unit suite **58/58** green (was 42 — +13 projector happy-path, +3 deserializer).

---

## Task 8 — Integration Tests
**Status**: DONE

End-to-end projection tests against a real SQL Server (LocalDB), exercising behaviour the
EF InMemory provider cannot honour: the handler's real per-batch transaction, the `Version`
optimistic-concurrency token, and the database-level `ON DELETE CASCADE` foreign keys. Events
are projected by sending `IncomingEvents` through `IMediator` (same path the Service Bus
listener uses), then asserted via the read model and the gRPC query stack.

**Coverage map (which test covers each bullet)**:

| Task 8 bullet | Covered by |
|---|---|
| Full event sequence: Create → AddOptions → AddCategories → Publish → query/verify | `FullEventSequence_CreateOptionsCategoriesPublish_ProjectsAndIsQueryable` — projects all 5 versions in one batch, then reads back via gRPC `GetAsync` (Status=Published, Version=5, 2 options, 2 categories) |
| Full lifecycle: Create → Publish → Deprecate → RemoveDeprecation → Disable → Delete | `FullLifecycle_CreateThroughDelete_TransitionsThenRemoves` — projects up to Disable (V5), asserts Status=Disabled + reason set + deprecation warning cleared (proving the V3→V4 transition survived the real round-trip), then projects Delete (V6) and asserts the row is gone |
| Concurrency token (Version) behavior with real DB | `ConcurrencyToken_StaleVersion_ThrowsDbUpdateConcurrencyException` — two contexts load the same V1 row; the first write wins, the second (stale original Version) throws `DbUpdateConcurrencyException`. InMemory ignores the token, so this is integration-only |
| Cascade deletes | `Delete_WithoutLoadingChildren_DatabaseCascadeRemovesOptionsAndCategories` — deletes ONLY the parent row (children never loaded, so EF cannot delete them); both child tables go to zero, proving the DB-level `ON DELETE CASCADE` FK. Also exercised end-to-end via the lifecycle test's `AttributeDeleted` |
| SortOrder management through add/reorder/remove | `SortOrder_AddReorderRemove_ProjectsExpectedSortOrders` — add small/medium/large (0/1/2) → reorder large,small,medium (0/1/2) → remove small; asserts large=0, medium=2 (removal leaves a gap, no recompaction) |

**New file** (`test/AnisShop.Attributes.Queries.IntegrationTests/EventsHandler/`):
- `EventProjectionIntegrationTest.cs` — 5 tests extending `SqlIntegrationTestBase` (LocalDB +
  Respawn reset between tests). Seeds via `EventHistoryBuilder` + `MediatorHelper.SendEvents`,
  asserts via `AssertAttributeState` / the gRPC client. Honours the no-hardcoded-Arabic rule
  (Latin placeholders: `"Arabic Red"`, `"Arabic Deprecation Warning"`, …).

**Test-infrastructure fixes** (these unblocked the whole integration suite, which was red before
this task — see below):
- `SqlIntegrationTestBase.cs` — (1) added a `MediatorHelper` (parallel to the existing
  `GrpcClientHelper`/`DatabaseHelper`) so integration tests can project events; (2) now calls
  `services.RemoveServiceBusServices()` in `ConfigureSqlServerEnvironment`.
- `test/.../Tests/Helpers/ServiceCollectionExtensions.cs` — promoted `RemoveServiceBusServices`
  from a `private` helper to a `public` reusable extension so BOTH the unit (InMemory) and
  integration (LocalDB) environments share one implementation. No behaviour change for unit tests.
- `Fixtures/LocalDbFixture.cs` — `InitializeAsync` now does `EnsureDeletedAsync()` **then**
  `EnsureCreatedAsync()`. `EnsureCreatedAsync` alone is a no-op when the DB already exists, so a
  database left on disk from an earlier session with an older model was missing the deprecation/
  disable columns (Task 2) and every insert failed with SQL 207 "invalid column name". Drop-then-
  create keeps the "model-driven schema, not migrations" decision while making it self-healing.

**Pre-existing break this task uncovered & fixed**: the entire integration suite (all 17 query
tests too) was failing at host boot — when the Service Bus listener was added (Task 3), the
integration base was never updated to strip the live listener + `ServiceBusClient` the way the
unit environment already did. With an empty connection string the `ServiceBusClient` singleton
threw at host start. Removing the Service Bus services in the integration base (mirroring the unit
environment) fixed it. No `src` changes — all fixes are test-only.

**Verified**: integration suite **22/22** green (5 new + 17 previously-red query tests now boot);
unit suite still **58/58** green (the `ServiceCollectionExtensions` change is non-breaking).
