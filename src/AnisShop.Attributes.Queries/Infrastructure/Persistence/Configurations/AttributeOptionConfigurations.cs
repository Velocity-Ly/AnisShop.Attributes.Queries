using AnisShop.Attributes.Queries.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AnisShop.Attributes.Queries.Infrastructure.Persistence.Configurations
{
    public class AttributeOptionConfigurations : IEntityTypeConfiguration<AttributeOption>
    {
        public void Configure(EntityTypeBuilder<AttributeOption> builder)
        {
            builder.HasKey(x => new { x.AttributeId, x.Key });

            builder.Property(x => x.Key).HasMaxLength(128).IsRequired();
            builder.Property(x => x.ArabicLabel).HasMaxLength(128).IsRequired();
            builder.Property(x => x.EnglishLabel).HasMaxLength(128).IsRequired();

            builder.HasIndex(x => new { x.AttributeId, x.SortOrder });
        }
    }
}
