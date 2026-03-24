using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Domain
{
    public class AttributeOptionFaker : NonPublicConstructorFaker<SourceDomain.AttributeOption>
    {
        public AttributeOptionFaker(Guid attributeId)
        {
            RuleFor(x => x.AttributeId, attributeId);
            RuleFor(x => x.Key, f => f.Random.AlphaNumeric(10));
            RuleFor(x => x.ArabicLabel, f => f.Commerce.ProductAdjective());
            RuleFor(x => x.EnglishLabel, f => f.Commerce.ProductAdjective());
            RuleFor(x => x.IsDisabled, f => f.Random.Bool());
            RuleFor(x => x.SortOrder, f => f.Random.Int(1, 100));
        }

        public AttributeOptionFaker WithDisabled(bool isDisabled)
        {
            RuleFor(x => x.IsDisabled, isDisabled);
            return this;
        }
    }
}
