using AnisShop.Attributes.Queries.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AnisShop.Attributes.Queries.Infrastructure.Persistence.Configurations
{
    public class AttributeCategoryConfigurations : IEntityTypeConfiguration<AttributeCategory>
    {
        public void Configure(EntityTypeBuilder<AttributeCategory> builder)
        {
            builder.HasKey(x => new { x.AttributeId, x.CategoryId });

            builder.HasIndex(x => x.CategoryId);
        }
    }
}
