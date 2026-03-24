using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Domain
{
    public class AttributeCategoryFaker : NonPublicConstructorFaker<SourceDomain.AttributeCategory>
    {
        public AttributeCategoryFaker(Guid attributeId)
        {
            RuleFor(x => x.AttributeId, attributeId);
            RuleFor(x => x.CategoryId, f => f.Random.Int(1, 1000));
        }

        public AttributeCategoryFaker WithCategoryId(int categoryId)
        {
            RuleFor(x => x.CategoryId, categoryId);
            return this;
        }
    }
}
