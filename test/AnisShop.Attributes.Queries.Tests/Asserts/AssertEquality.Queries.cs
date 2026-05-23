using AnisShop.Attributes.Queries.Tests.QueriesProto;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.Tests.Asserts
{
    public static partial class AssertEquality
    {
        public static void OfDomainAndQueryResponse(SourceDomain.Attribute[] attributes, GetByCategoryResponse response)
        {
            Assert.Equal(attributes.Length, response.Attributes.Count);

            foreach (var attribute in attributes)
            {
                var matchingAttribute = Assert.Single(response.Attributes, a => a.Id == attribute.Id.ToString());
                Assert.Equal(attribute.ArabicDisplayName, matchingAttribute.ArabicDisplayName);
                Assert.Equal(attribute.EnglishDisplayName, matchingAttribute.EnglishDisplayName);
                Assert.Equal(attribute.ArabicDescription, matchingAttribute.ArabicDescription);
                Assert.Equal(attribute.EnglishDescription, matchingAttribute.EnglishDescription);
                Assert.Equal((int)attribute.Type, (int)matchingAttribute.Type);
                Assert.Equal((int)attribute.Status, (int)matchingAttribute.Status);
                Assert.Equal(attribute.ArabicDeprecationWarning, matchingAttribute.ArabicDeprecationWarning);
                Assert.Equal(attribute.EnglishDeprecationWarning, matchingAttribute.EnglishDeprecationWarning);
                Assert.Equal(attribute.ArabicDisableReason, matchingAttribute.ArabicDisableReason);
                Assert.Equal(attribute.EnglishDisableReason, matchingAttribute.EnglishDisableReason);
                Assert.Equal(attribute.Version, matchingAttribute.Version);

                OfOptions(attribute.Options, matchingAttribute.Options);
                OfCategories(attribute.ApplicableCategories, matchingAttribute.ApplicableCategoryIds);
            }
        }

        public static void OfPagination(GetByCategoryResponse response, int expectedCurrentPage, int expectedPageSize, int expectedTotal)
        {
            Assert.Equal(expectedCurrentPage, response.CurrentPage);
            Assert.Equal(expectedPageSize, response.PageSize);
            Assert.Equal(expectedTotal, response.Attributes.Count);
        }
    }
}
