using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Events;
using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Mediator;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Polly;
using Polly.Retry;
using Attribute = AnisShop.Attributes.Queries.Domain.Attribute;

namespace AnisShop.Attributes.Queries.Features.EventsHandler
{
    public class IncomingEventsHandler : IRequestHandler<IncomingEvents, bool>
    {
        // Shared, thread-safe pipeline that retries only transient SQL faults (deadlock
        // victim, dropped connection, request timeout). DbUpdateConcurrencyException is a
        // genuine conflict, not transient, so it is deliberately NOT matched — it bubbles
        // out, the batch is left uncompleted, and Service Bus redelivers it for a fresh,
        // idempotent re-projection.
        private static readonly ResiliencePipeline ProjectionPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<SqlException>(ex => ex.IsTransient)
                    .Handle<DbUpdateException>(ex => ex.InnerException is SqlException { IsTransient: true }),
                MaxRetryAttempts = 3,
                Delay = TimeSpan.FromMilliseconds(200),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
            })
            .Build();

        private readonly AttributesDbContext _dbContext;

        public IncomingEventsHandler(AttributesDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async ValueTask<bool> Handle(IncomingEvents request, CancellationToken cancellationToken)
        {
            if (request.Events.Count == 0)
                return true;

            var orderedEvents = request.Events.OrderBy(e => e.Version).ToList();
            var aggregateId = orderedEvents[0].AggregateId;

            var attribute = await _dbContext.Attributes
                .Include(a => a.Options)
                .Include(a => a.ApplicableCategories)
                .SingleOrDefaultAsync(a => a.Id == aggregateId, cancellationToken);

            var currentVersion = attribute?.Version ?? 0;

            foreach (var @event in orderedEvents)
            {
                // Already projected — idempotent skip (replay/duplicate delivery).
                if (@event.Version <= currentVersion)
                    continue;

                // Contiguity gap — refuse the batch so Service Bus redelivers it later,
                // once the missing version(s) have arrived.
                if (@event.Version != currentVersion + 1)
                    return false;

                attribute = Apply(@event, attribute);
                currentVersion = @event.Version;
            }

            await PersistBatchAsync(cancellationToken);
            return true;
        }

        // Commits the whole batch as one transaction, retrying transient SQL faults.
        // The load + Apply pass above runs ONCE, outside the retry, so a retry never
        // re-queries or re-tracks (which would duplicate inserts); only the persistence
        // step is repeated against the already-built change set.
        private async ValueTask PersistBatchAsync(CancellationToken cancellationToken)
        {
            await ProjectionPipeline.ExecuteAsync(async ct =>
            {
                await using var transaction = await _dbContext.BeginTransactionAsync(ct);

                // acceptAllChangesOnSuccess: false keeps the change tracker dirty until the
                // transaction actually commits. If Commit fails transiently, the retry
                // re-sends the same changes instead of silently committing an empty batch.
                await _dbContext.SaveChangesAsync(acceptAllChangesOnSuccess: false, ct);
                await transaction.CommitAsync(ct);
                _dbContext.ChangeTracker.AcceptAllChanges();
            }, cancellationToken);
        }

        private Attribute? Apply(EventBase @event, Attribute? attribute)
        {
            switch (@event)
            {
                case AttributeCreated created:
                    attribute = Attribute.Create(
                        created.AggregateId,
                        created.Data.Metadata.ArabicDisplayName,
                        created.Data.Metadata.EnglishDisplayName,
                        created.Data.Metadata.ArabicDescription,
                        created.Data.Metadata.EnglishDescription,
                        ParseType(created.Data.Type),
                        created.Version);
                    _dbContext.Attributes.Add(attribute);
                    return attribute;

                case AttributePublished published:
                    attribute!.Publish(published.Version);
                    return attribute;

                case AttributeMetadataChanged metadataChanged:
                    attribute!.ChangeMetadata(
                        metadataChanged.Data.Metadata.ArabicDisplayName,
                        metadataChanged.Data.Metadata.EnglishDisplayName,
                        metadataChanged.Data.Metadata.ArabicDescription,
                        metadataChanged.Data.Metadata.EnglishDescription,
                        metadataChanged.Version);
                    return attribute;

                case AttributeTypeChanged typeChanged:
                    attribute!.ChangeType(ParseType(typeChanged.Data.Type), typeChanged.Version);
                    return attribute;

                case AttributeMarkedAsDeprecated deprecated:
                    attribute!.MarkAsDeprecated(
                        deprecated.Data.Warning.Arabic,
                        deprecated.Data.Warning.English,
                        deprecated.Version);
                    return attribute;

                case AttributeDeprecationWarningRemoved warningRemoved:
                    attribute!.RemoveDeprecationWarning(warningRemoved.Version);
                    return attribute;

                case AttributeDisabled disabled:
                    attribute!.Disable(
                        disabled.Data.Reason.Arabic,
                        disabled.Data.Reason.English,
                        disabled.Version);
                    return attribute;

                case AttributeApplicableCategoriesAdded categoriesAdded:
                    attribute!.AddCategories(categoriesAdded.Data.ApplicableCategoryIds, categoriesAdded.Version);
                    return attribute;

                case AttributeApplicableCategoriesRemoved categoriesRemoved:
                    attribute!.RemoveCategories(categoriesRemoved.Data.ApplicableCategoryIds, categoriesRemoved.Version);
                    return attribute;

                case AttributeDeleted:
                    _dbContext.Attributes.Remove(attribute!);
                    return null;

                case AttributeOptionAdded optionAdded:
                    attribute!.AddOption(
                        optionAdded.Data.Option.Key,
                        optionAdded.Data.Option.Label.Arabic,
                        optionAdded.Data.Option.Label.English,
                        optionAdded.Version);
                    return attribute;

                case AttributeOptionLabelChanged labelChanged:
                    attribute!.ChangeOptionLabel(
                        labelChanged.Data.Option.Key,
                        labelChanged.Data.Option.Label.Arabic,
                        labelChanged.Data.Option.Label.English,
                        labelChanged.Version);
                    return attribute;

                case AttributeOptionDisabled optionDisabled:
                    attribute!.DisableOption(optionDisabled.Data.Key, optionDisabled.Version);
                    return attribute;

                case AttributeOptionRemoved optionRemoved:
                    attribute!.RemoveOption(optionRemoved.Data.Key, optionRemoved.Version);
                    return attribute;

                case AttributeOptionsReordered optionsReordered:
                    attribute!.ReorderOptions(optionsReordered.Data.OrderedKeys, optionsReordered.Version);
                    return attribute;

                default:
                    throw new NotImplementedException($"No projector registered for event type '{@event.GetType().Name}'.");
            }
        }

        private static AttributeType ParseType(string type) =>
            Enum.Parse<AttributeType>(type, ignoreCase: true);
    }
}
