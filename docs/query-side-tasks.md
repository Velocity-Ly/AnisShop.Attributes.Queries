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
**Status**: NOT STARTED

The core consumer that listens to Azure Service Bus and routes events to projectors.

**Components**:
- `ServiceBusSessionProcessor` wrapped in `IHostedService`
  - Must use sessions (publisher sets SessionId = AggregateId for FIFO ordering)
- JSON deserialization (camelCase, System.Text.Json)
- Event type routing based on message `Type` application property
- Idempotency: only process if `event.Version == currentAttribute.Version + 1`
  - For `AttributeCreated`: no prior row expected, Version must be 1
- Error handling / dead-lettering for unprocessable messages
- DI registration in Program.cs
- Configuration model (connection string, topic name, subscription name)
- Polly retry for transient DB failures (package already referenced)

**Key design decisions**:
- Projector interface: each event type maps to a projector method
- Transaction wrapping: read model update + message completion in one logical unit
- Logging: structured logging via Serilog for each event processed

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
