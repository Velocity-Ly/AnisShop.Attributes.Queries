using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Domain
{
    public class AttributeTargetFaker : NonPublicConstructorFaker<SourceDomain.AttributeTarget>
    {
        public AttributeTargetFaker(Guid attributeId)
        {
            RuleFor(x => x.AttributeId, attributeId);
            RuleFor(x => x.TargetId, f => f.Random.Int(1, 1000));
        }

        public AttributeTargetFaker WithTargetId(int targetId)
        {
            RuleFor(x => x.TargetId, targetId);
            return this;
        }
    }
}
