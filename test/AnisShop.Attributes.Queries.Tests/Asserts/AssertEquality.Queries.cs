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
                Assert.Equal(attribute.Version, matchingAttribute.Version);
                Assert.Equal(attribute.Options.Count, matchingAttribute.Options.Count);
                Assert.Equal(attribute.ApplicableCategories.Count, matchingAttribute.ApplicableCategoryIds.Count);
            }
        }

        public static void OfPagination(GetByCategoryResponse response, int expectedCurrentPage, int expectedPageSize, int expectedTotal)
        {
            Assert.Equal(expectedCurrentPage, response.CurrentPage);
            Assert.Equal(expectedPageSize, response.PageSize);
        }
    }
}
