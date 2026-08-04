namespace AnisShop.Attributes.Queries.Domain
{
    public class AttributeTarget
    {
        private AttributeTarget(Guid attributeId, int targetId)
        {
            AttributeId = attributeId;
            TargetId = targetId;
        }

        public Guid AttributeId { get; private set; }
        public Attribute? Attribute { get; private set; }
        public int TargetId { get; private set; }

        internal static AttributeTarget Create(Guid attributeId, int targetId)
            => new(attributeId, targetId);
    }
}
