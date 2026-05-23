using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Fakers.Domain
{
    public class AttributeFaker : NonPublicConstructorFaker<SourceDomain.Attribute>
    {
        private int _optionsCount;
        private int[]? _categoryIds;

        public AttributeFaker()
        {
            RuleFor(x => x.Id, f => f.Random.Guid());
            RuleFor(x => x.ArabicDisplayName, f => f.Commerce.ProductName());
            RuleFor(x => x.EnglishDisplayName, f => f.Commerce.ProductName());
            RuleFor(x => x.ArabicDescription, f => f.Lorem.Sentence());
            RuleFor(x => x.EnglishDescription, f => f.Lorem.Sentence());
            RuleFor(x => x.Type, f => f.PickRandom<SourceDomain.AttributeType>());
            RuleFor(x => x.Status, f => f.PickRandom<SourceDomain.AttributeStatus>());
            RuleFor(x => x.ArabicDeprecationWarning, f => f.Lorem.Sentence());
            RuleFor(x => x.EnglishDeprecationWarning, f => f.Lorem.Sentence());
            RuleFor(x => x.ArabicDisableReason, f => f.Lorem.Sentence());
            RuleFor(x => x.EnglishDisableReason, f => f.Lorem.Sentence());
            RuleFor(x => x.Version, f => f.Random.Int(1, 100));
        }

        public AttributeFaker WithStatus(SourceDomain.AttributeStatus status)
        {
            RuleFor(x => x.Status, status);
            return this;
        }

        public AttributeFaker WithType(SourceDomain.AttributeType type)
        {
            RuleFor(x => x.Type, type);
            return this;
        }

        public AttributeFaker WithOptions(int count)
        {
            _optionsCount = count;
            return this;
        }

        public AttributeFaker WithVersion(int version)
        {
            RuleFor(x => x.Version, version);
            return this;
        }

        public AttributeFaker WithCategoryIds(params int[] categoryIds)
        {
            _categoryIds = categoryIds;
            return this;
        }

        public override SourceDomain.Attribute Generate(string? ruleSets = null)
        {
            var attribute = base.Generate(ruleSets);

            if (_optionsCount > 0)
            {
                var options = new AttributeOptionFaker(attribute.Id).Generate(_optionsCount);
                foreach (var option in options)
                {
                    attribute.Options.Add(option);
                }
            }

            if (_categoryIds != null)
            {
                foreach (var categoryId in _categoryIds)
                {
                    var category = new AttributeCategoryFaker(attribute.Id)
                        .WithCategoryId(categoryId)
                        .Generate();
                    attribute.ApplicableCategories.Add(category);
                }
            }

            return attribute;
        }
    }
}
