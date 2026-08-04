# Commands ↔ Queries Relationship Doc

> **Purpose**: Internal reference for Claude when doing feature work on the Queries project.
> This doc captures the architectural relationship, event contracts, gaps, and notes.

---

## 1. Architecture Overview

These two projects follow **CQRS + Event Sourcing**:

- **Commands** (`AnisShop.Attributes.Commands`): Receives gRPC commands, validates domain rules via discriminated-union aggregate (`AttributeBase`), appends events to an event store (EF Core + SQL Server), and publishes them to **Azure Service Bus** via an **outbox pattern**.
- **Queries** (`AnisShop.Attributes.Queries`): Maintains a **flat relational read model** (SQL Server) and exposes gRPC query endpoints. It is meant to consume events from Service Bus and project them into its read model.

The **only direct contract** between them is the **set of events** and the **domain logic they represent**.

---

## 2. Event Contract (The Bridge)

All events inherit from a base `Event` record with: `AggregateId`, `Version`, `UserId`, `DateTime`.

Service Bus message properties set by the publisher:
- `AggregateId` → SessionId + PartitionKey (guarantees ordered processing per aggregate)
- `Type` → event type discriminator string
- `DateTime`, `UserId`, `Version` → metadata properties
- Body → JSON serialized with **camelCase** naming, **null values ignored**

### Full Event Catalog

> **2026-05-25 — "targets + scope" refactor (Commands commit `62fa3da`).** "Applicable categories" was generalized to "applicable **targets**": `CategoryId` → `TargetId`, and the two `…ApplicableCategories…` events became `…ApplicableTargets…`. A new `AttributeScope` (`ProductCategory` / `MarketType`) distinguishes what the target ids refer to, and `AttributeCreated` now carries it. The Queries side mirrors this end-to-end (read model `AttributeTarget` + `Attribute.Scope`, `GetByTarget` gRPC endpoint). The table/columns below reflect the post-refactor contract.

| Event | Data Payload | Command Status | Query Projection Status |
|---|---|---|---|
| `AttributeCreated` | Metadata, Type, **Scope** | **Implemented** | **Implemented** |
| `AttributePublished` | (none) | **Implemented** | **Implemented** |
| `AttributeOptionAdded` | AttributeOption (Key + Labels) | **Implemented** | **Implemented** |
| `AttributeApplicableTargetsAdded` | IReadOnlyCollection\<TargetId\> | **Implemented** | **Implemented** |
| `AttributeMetadataChanged` | AttributeMetadata | **Implemented** | **Implemented** |
| `AttributeDeleted` | (none) | (commands status varies) | **Implemented** |
| `AttributeMarkedAsDeprecated` | AttributeDeprecationWarning | (commands status varies) | **Implemented** |
| `AttributeDeprecationWarningRemoved` | (none) | (commands status varies) | **Implemented** |
| `AttributeDisabled` | AttributeDisableReason | (commands status varies) | **Implemented** |
| `AttributeOptionRemoved` | AttributeOptionKey | (commands status varies) | **Implemented** |
| `AttributeOptionDisabled` | AttributeOptionKey | (commands status varies) | **Implemented** |
| `AttributeOptionLabelChanged` | AttributeOption | (commands status varies) | **Implemented** |
| `AttributeOptionsReordered` | NonEmptyCollection\<AttributeOptionKey\> | (commands status varies) | **Implemented** |
| `AttributeTypeChanged` | AttributeType | (commands status varies) | **Implemented** |
| `AttributeApplicableTargetsRemoved` | ICollection\<TargetId\> | (commands status varies) | **Implemented** |

---

## 3. Domain Model Mapping (Events → Read Model)

### Commands Domain (Event-Sourced Aggregate)
```
AttributeBase (discriminated union)
├── NoAttribute
├── ExistingAttribute { Id, Metadata, Type, Scope, ApplicableTargets, Options, IsPublished }
│   ├── DeprecatedAttribute { ..., DeprecationWarning }
│   └── DisabledAttribute { ..., DisableReason }
└── DeletedAttribute { Id }
```
`Scope` is an `AttributeScope` value (`ProductCategory` / `MarketType`); `ApplicableTargets` is a set of `TargetId` (int) interpreted within that scope.

### Queries Read Model (Relational)
```
Attribute { Id, ArabicDisplayName, EnglishDisplayName, ArabicDescription, EnglishDescription, Type, Scope, Status, DeprecationWarning(ar/en), DisableReason(ar/en), Version }
├── AttributeOption { AttributeId, Key, ArabicLabel, EnglishLabel, IsDisabled, SortOrder }
└── AttributeTarget { AttributeId, TargetId }
```

### How Events Should Map to Read Model Updates

| Event | Read Model Mutation |
|---|---|
| `AttributeCreated` | INSERT Attribute (Status=Draft, Version=1, Scope set) |
| `AttributeOptionAdded` | INSERT AttributeOption (SortOrder = next increment) |
| `AttributeApplicableTargetsAdded` | INSERT AttributeTarget rows |
| `AttributePublished` | UPDATE Attribute SET Status=Published |
| `AttributeMetadataChanged` | UPDATE Attribute display names & descriptions |
| `AttributeDeleted` | DELETE Attribute (cascade deletes options & targets) |
| `AttributeMarkedAsDeprecated` | UPDATE Attribute SET Status=Deprecated |
| `AttributeDeprecationWarningRemoved` | UPDATE Attribute SET Status=Published (back from deprecated) |
| `AttributeDisabled` | UPDATE Attribute SET Status=Disabled |
| `AttributeOptionRemoved` | DELETE AttributeOption row |
| `AttributeOptionDisabled` | UPDATE AttributeOption SET IsDisabled=true |
| `AttributeOptionLabelChanged` | UPDATE AttributeOption labels |
| `AttributeOptionsReordered` | UPDATE SortOrder for each option |
| `AttributeTypeChanged` | UPDATE Attribute SET Type=newType |
| `AttributeApplicableTargetsRemoved` | DELETE AttributeTarget rows |

**Important**: Always UPDATE `Attribute.Version` to the event's Version on every event.

---

## 4. Critical Gaps & Missing Pieces

### 4.1 NO EVENT CONSUMER EXISTS IN QUERIES
The Queries project has `Azure.Messaging.ServiceBus` as a dependency but **there is no event consumer / processor implementation**. The read model has no way to be populated from events. This is the single biggest missing piece.

To implement, you need:
- A `ServiceBusProcessor` or `ServiceBusSessionProcessor` (since Commands uses SessionId = AggregateId for ordering)
- An `IHostedService` to manage the processor lifecycle
- Event deserialization (camelCase JSON, matching the publisher's format)
- Event handler/projector that applies each event to the read model
- Idempotency via the Version field (skip if already processed)
- Transaction wrapping (read model update + checkpoint in one transaction)

### 4.2 Event Type Definitions Not Shared
The event classes are defined in the Commands project. The Queries project does NOT reference the Commands project (correctly, to maintain CQRS separation). This means:
- Queries needs its **own event DTOs** for deserialization
- These must match the JSON shape published by Commands (camelCase, nulls ignored)
- The discriminator is the `Type` message property on the Service Bus message

### 4.3 Version / Idempotency
- Commands enforces unique `(AggregateId, Version)` in its event store
- Queries has a `Version` column on the `Attribute` table (configured as concurrency token)
- The consumer should use Version for idempotency: only process event if `event.Version == currentVersion + 1`
- Out-of-order delivery shouldn't happen within a session (Service Bus sessions guarantee FIFO per session), but idempotency is still needed for reprocessing after failures

### 4.4 SortOrder for Options
- Commands does NOT track option sort order; options are stored as an `IImmutableDictionary<Key, Label>`
- Queries DOES have a `SortOrder` column on `AttributeOption`
- The `AttributeOptionsReordered` event exists in the catalog but is not implemented
- When projecting `AttributeOptionAdded`, the consumer must assign SortOrder (likely auto-increment based on current count)

### 4.5 Status Mapping
Commands uses aggregate type discrimination (ExistingAttribute vs DeprecatedAttribute vs DisabledAttribute), with a separate `IsPublished` bool. Queries uses a flat `Status` enum: Draft, Published, Deprecated, Disabled.

Mapping logic:
- `ExistingAttribute` + `IsPublished=false` → Draft
- `ExistingAttribute` + `IsPublished=true` → Published
- `DeprecatedAttribute` → Deprecated
- `DisabledAttribute` → Disabled
- `DeletedAttribute` → row deleted

But since the consumer processes events (not aggregate snapshots), the mapping is simpler:
- After `AttributeCreated` → Draft
- After `AttributePublished` → Published
- After `AttributeMarkedAsDeprecated` → Deprecated
- After `AttributeDeprecationWarningRemoved` → Published
- After `AttributeDisabled` → Disabled

### 4.6 DeprecationWarning and DisableReason Not in Read Model
The Commands domain carries `AttributeDeprecationWarning` and `AttributeDisableReason` (both bilingual). The Queries read model does NOT have columns for these. If these are needed for display, the read model schema needs extending.

---

## 5. Technology Stack Alignment

| Concern | Commands | Queries |
|---|---|---|
| Framework | .NET 10.0 | .NET 10.0 |
| Transport | gRPC | gRPC |
| Database | SQL Server (EF Core) | SQL Server (EF Core) |
| Messaging | Azure Service Bus (publisher) | Azure Service Bus (consumer - NOT IMPLEMENTED) |
| Mediation | Mediator (SourceGen) | Mediator (SourceGen) |
| Validation | Value objects (SourceGen) | FluentValidation |
| Logging | Serilog | Serilog |
| Resilience | Polly (retry on DbUpdateException) | Not yet needed |
| Serialization | System.Text.Json (camelCase) | Needs matching deserialization |

---

## 6. Value Object Differences

Commands uses rich value objects via SourceGen (`AttributeId`, `CategoryId`, `AttributeOptionKey`, etc.).
Queries uses plain primitives (`Guid`, `int`, `string`). This is correct for the read side.

When building the event consumer, deserialize directly to simple DTOs, not to Commands' value objects.

---

## 7. Existing Query Endpoints

| Endpoint | Input | Output | Notes |
|---|---|---|---|
| `Get` | `id` (string/Guid) | Full attribute with options + targets | Eager loads Options (ordered by SortOrder) and ApplicableTargets; output carries `scope` + `applicable_target_ids` |
| `GetByTarget` | `scope`, `target_id`, `current_page`, `page_size` | Paginated list of attributes | Filters by **both** scope AND target id (`a.Scope == scope && a.ApplicableTargets.Any(t => t.TargetId == target_id)`) — target ids are only meaningful within a scope, so scope is required |

Proto enums exposed:
- `AttributeType`: UNSPECIFIED, SINGLE_SELECT, MULTI_SELECT
- `AttributeStatus`: UNSPECIFIED, DRAFT, PUBLISHED, DEPRECATED, DISABLED
- `AttributeScope`: UNSPECIFIED, PRODUCT_CATEGORY, MARKET_TYPE

---

## 8. Testing Patterns

- Unit tests use `WebApplicationFactory<Program>` with EF Core InMemory
- Integration tests use SQL Server LocalDB via `LocalDbFixture`
- Fakers (Bogus library) exist for: Attribute, AttributeOption, AttributeCategory, GetRequest, GetByCategoryRequest
- Custom assert helpers in `AssertEquality` classes
- GrpcClientHelper creates typed gRPC clients for testing

When adding event consumer tests:
- Test each event projection independently
- Test idempotency (replaying same event)
- Test ordering (Version sequence)
- Consider adding an `EventFaker` for test data generation

---

## 9. File Locations Quick Reference

### Commands Project
- Events: `src/AnisShop.Attributes.Commands/Events/`
- Domain aggregate: `src/AnisShop.Attributes.Commands/Domain/AttributeBase.cs`
- Event application: `src/AnisShop.Attributes.Commands/Domain/AttributeEventsOperations.cs`
- Command operations: `src/AnisShop.Attributes.Commands/Domain/AttributeCommandsOperations.cs`
- Publisher: `src/AnisShop.Attributes.Commands/Infrastructure/MessageBus/ServiceBusPublisher.cs`
- Outbox: `src/AnisShop.Attributes.Commands/Infrastructure/Persistence/OutboxMessage.cs`

### Queries Project
- Read models: `src/AnisShop.Attributes.Queries/Domain/`
- DB context: `src/AnisShop.Attributes.Queries/Infrastructure/Persistence/AttributesDbContext.cs`
- DB configs: `src/AnisShop.Attributes.Queries/Infrastructure/Persistence/Configurations/`
- Query handlers: `src/AnisShop.Attributes.Queries/Features/Queries/`
- gRPC service: `src/AnisShop.Attributes.Queries/GrpcServices/AttributesQueriesService.cs`
- Extensions/mapping: `src/AnisShop.Attributes.Queries/Extensions/QueriesExtensions.cs`
- Proto: `src/AnisShop.Attributes.Queries/Protos/attributes_queries.proto`

---

## 10. Notes for Future Feature Work

1. **Building the event consumer is the #1 priority** — without it the read model is empty.
2. The consumer must use **Service Bus sessions** (not regular consumer) because the publisher sets `SessionId = AggregateId`.
3. JSON deserialization must match: `camelCase` property naming, nulls excluded, `System.Text.Json`.
4. The Queries project should define its own event DTOs (not reference Commands).
5. Consider whether `DeprecationWarning` and `DisableReason` need to be added to the read model.
6. The `SortOrder` assignment for options needs a strategy since Commands doesn't track it.
7. When implementing new commands on the Commands side, always check if the corresponding projection exists on the Queries side.
8. The `Version` concurrency token on the Attribute entity is already configured — use it for idempotent projections.
