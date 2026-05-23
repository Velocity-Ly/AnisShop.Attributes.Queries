using AnisShop.Attributes.Queries.Tests.QueriesProto;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Asserts
{
    public static partial class AssertEquality
    {
        public static void OfDomainAndResponse(SourceDomain.Attribute attribute, GetResponse response)
        {
            Assert.NotNull(response.Attribute);
            Assert.Equal(attribute.Id.ToString(), response.Attribute.Id);
            Assert.Equal(attribute.ArabicDisplayName, response.Attribute.ArabicDisplayName);
            Assert.Equal(attribute.EnglishDisplayName, response.Attribute.EnglishDisplayName);
            Assert.Equal(attribute.ArabicDescription, response.Attribute.ArabicDescription);
            Assert.Equal(attribute.EnglishDescription, response.Attribute.EnglishDescription);
            Assert.Equal((int)attribute.Type, (int)response.Attribute.Type);
            Assert.Equal((int)attribute.Status, (int)response.Attribute.Status);
            Assert.Equal(attribute.ArabicDeprecationWarning, response.Attribute.ArabicDeprecationWarning);
            Assert.Equal(attribute.EnglishDeprecationWarning, response.Attribute.EnglishDeprecationWarning);
            Assert.Equal(attribute.ArabicDisableReason, response.Attribute.ArabicDisableReason);
            Assert.Equal(attribute.EnglishDisableReason, response.Attribute.EnglishDisableReason);
            Assert.Equal(attribute.Version, response.Attribute.Version);

            OfOptions(attribute.Options, response.Attribute.Options);
            OfCategories(attribute.ApplicableCategories, response.Attribute.ApplicableCategoryIds);
        }

        public static void OfOptions(ICollection<SourceDomain.AttributeOption> expected, IEnumerable<AttributeOptionOutput> actual)
        {
            var actualList = actual.ToList();
            Assert.Equal(expected.Count, actualList.Count);

            foreach (var expectedOption in expected)
            {
                var matchingOption = Assert.Single(actualList, o => o.Key == expectedOption.Key);
                Assert.Equal(expectedOption.ArabicLabel, matchingOption.ArabicLabel);
                Assert.Equal(expectedOption.EnglishLabel, matchingOption.EnglishLabel);
                Assert.Equal(expectedOption.IsDisabled, matchingOption.IsDisabled);
            }
        }

        public static void OfCategories(ICollection<SourceDomain.AttributeCategory> expected, IEnumerable<int> actual)
        {
            var actualList = actual.ToList();
            Assert.Equal(expected.Count, actualList.Count);

            foreach (var expectedCategory in expected)
            {
                Assert.Contains(expectedCategory.CategoryId, actualList);
            }
        }
    }
}
