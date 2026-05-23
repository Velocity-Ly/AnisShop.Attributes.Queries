namespace AnisShop.Attributes.Queries.Domain
{
    public class AttributeCategory
    {
        private AttributeCategory(Guid attributeId, int categoryId)
        {
            AttributeId = attributeId;
            CategoryId = categoryId;
        }

        public Guid AttributeId { get; private set; }
        public Attribute? Attribute { get; private set; }
        public int CategoryId { get; private set; }

        internal static AttributeCategory Create(Guid attributeId, int categoryId)
            => new(attributeId, categoryId);
    }
}
