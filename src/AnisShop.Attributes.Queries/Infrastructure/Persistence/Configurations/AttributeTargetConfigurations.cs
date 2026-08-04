using AnisShop.Attributes.Queries.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AnisShop.Attributes.Queries.Infrastructure.Persistence.Configurations
{
    public class AttributeTargetConfigurations : IEntityTypeConfiguration<AttributeTarget>
    {
        public void Configure(EntityTypeBuilder<AttributeTarget> builder)
        {
            builder.HasKey(x => new { x.AttributeId, x.TargetId });

            builder.HasIndex(x => x.TargetId);
        }
    }
}
