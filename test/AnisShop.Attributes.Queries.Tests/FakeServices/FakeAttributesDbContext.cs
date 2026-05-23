using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace AnisShop.Attributes.Queries.Tests.FakeServices;

public class FakeAttributesDbContext(DbContextOptions<AttributesDbContext> options)
    : AttributesDbContext(options)
{
    public override Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IDbContextTransaction>(new FakeTransaction());

    // The InMemory provider has no real transaction, and the handler persists with
    // SaveChanges(acceptAllChangesOnSuccess: false) BEFORE calling Commit — so the rows are
    // already in the store while the entities stay dirty. Commit must therefore be a no-op:
    // re-calling SaveChanges here would re-insert the still-Added entities (duplicate key).
    private class FakeTransaction : IDbContextTransaction
    {
        public Guid TransactionId => Guid.NewGuid();

        public Task CommitAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task RollbackAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Commit()
        {
        }

        public void Rollback()
        {
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
