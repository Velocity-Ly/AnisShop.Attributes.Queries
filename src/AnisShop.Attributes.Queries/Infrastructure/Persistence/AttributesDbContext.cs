using AnisShop.Attributes.Queries.Domain;
using AnisShop.Attributes.Queries.Infrastructure.Persistence.Configurations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data;

namespace AnisShop.Attributes.Queries.Infrastructure.Persistence
{
    public class AttributesDbContext(DbContextOptions<AttributesDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfiguration(new AttributeConfigurations());
            modelBuilder.ApplyConfiguration(new AttributeOptionConfigurations());
            modelBuilder.ApplyConfiguration(new AttributeTargetConfigurations());
        }

        public DbSet<Domain.Attribute> Attributes { get; set; }
        public DbSet<AttributeOption> AttributeOptions { get; set; }
        public DbSet<AttributeTarget> AttributeTargets { get; set; }

        public virtual Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken) => Database.IsSqlServer()
                ? Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
                : Database.BeginTransactionAsync(cancellationToken);
    }
}
