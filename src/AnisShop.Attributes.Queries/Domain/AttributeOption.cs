namespace AnisShop.Attributes.Queries.Domain
{
    public class AttributeOption
    {
        private AttributeOption(
            Guid attributeId,
            string key,
            string arabicLabel,
            string englishLabel,
            bool isDisabled,
            int sortOrder)
        {
            AttributeId = attributeId;
            Key = key;
            ArabicLabel = arabicLabel;
            EnglishLabel = englishLabel;
            IsDisabled = isDisabled;
            SortOrder = sortOrder;
        }

        public Guid AttributeId { get; private set; }
        public Attribute? Attribute { get; private set; }
        public string Key { get; private set; }
        public string ArabicLabel { get; private set; }
        public string EnglishLabel { get; private set; }
        public bool IsDisabled { get; private set; }
        public int SortOrder { get; private set; }

        internal static AttributeOption Create(
            Guid attributeId,
            string key,
            string arabicLabel,
            string englishLabel,
            int sortOrder)
            => new(attributeId, key, arabicLabel, englishLabel, isDisabled: false, sortOrder);

        internal void ChangeLabel(string arabicLabel, string englishLabel)
        {
            ArabicLabel = arabicLabel;
            EnglishLabel = englishLabel;
        }

        internal void Disable() => IsDisabled = true;

        internal void SetSortOrder(int sortOrder) => SortOrder = sortOrder;
    }
}
