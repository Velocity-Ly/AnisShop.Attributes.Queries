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
            Assert.Equal(attribute.Version, response.Attribute.Version);
        }
    }
}
