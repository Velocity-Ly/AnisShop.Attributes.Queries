using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AnisShop.Attributes.Queries.Infrastructure.Persistence.Configurations
{
    public class AttributeConfigurations : IEntityTypeConfiguration<Domain.Attribute>
    {
        public void Configure(EntityTypeBuilder<Domain.Attribute> builder)
        {
            builder.HasKey(x => x.Id);

            builder.Property(x => x.Version).IsConcurrencyToken();
            builder.Property(x => x.ArabicDisplayName).HasMaxLength(128).IsRequired();
            builder.Property(x => x.EnglishDisplayName).HasMaxLength(128).IsRequired();
            builder.Property(x => x.ArabicDescription).HasMaxLength(1000);
            builder.Property(x => x.EnglishDescription).HasMaxLength(1000);
            builder.Property(x => x.ArabicDeprecationWarning).HasMaxLength(1000);
            builder.Property(x => x.EnglishDeprecationWarning).HasMaxLength(1000);
            builder.Property(x => x.ArabicDisableReason).HasMaxLength(1000);
            builder.Property(x => x.EnglishDisableReason).HasMaxLength(1000);

            builder.HasIndex(x => x.ArabicDisplayName);
            builder.HasIndex(x => x.EnglishDisplayName);

            builder.HasMany(x => x.Options)
                .WithOne(x => x.Attribute)
                .HasForeignKey(x => x.AttributeId)
                .OnDelete(DeleteBehavior.Cascade);

            builder.HasMany(x => x.ApplicableTargets)
                .WithOne(x => x.Attribute)
                .HasForeignKey(x => x.AttributeId)
                .OnDelete(DeleteBehavior.Cascade);
        }
    }
}
