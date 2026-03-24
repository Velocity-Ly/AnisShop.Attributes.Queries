using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace AnisShop.Attributes.Queries.Tests.FakeServices;

public class FakeAttributesDbContext(DbContextOptions<AttributesDbContext> options)
    : AttributesDbContext(options)
{
    public override Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IDbContextTransaction>(new FakeTransaction(this));

    private class FakeTransaction(DbContext context) : IDbContextTransaction
    {
        private bool _committed;

        public Guid TransactionId => Guid.NewGuid();

        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (!_committed)
            {
                await context.SaveChangesAsync(cancellationToken);
                _committed = true;
            }
        }

        public Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public void Commit()
        {
            if (!_committed)
            {
                context.SaveChanges();
                _committed = true;
            }
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
