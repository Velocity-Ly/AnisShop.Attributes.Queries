namespace AnisShop.Attributes.Queries.Domain
{
    public class Attribute
    {
        private Attribute(
            Guid id,
            string arabicDisplayName,
            string englishDisplayName,
            string? arabicDescription,
            string? englishDescription,
            AttributeType type,
            AttributeStatus status,
            int version)
        {
            Id = id;
            ArabicDisplayName = arabicDisplayName;
            EnglishDisplayName = englishDisplayName;
            ArabicDescription = arabicDescription;
            EnglishDescription = englishDescription;
            Type = type;
            Status = status;
            Version = version;
        }

        public Guid Id { get; private set; }
        public string ArabicDisplayName { get; private set; }
        public string EnglishDisplayName { get; private set; }
        public string? ArabicDescription { get; private set; }
        public string? EnglishDescription { get; private set; }
        public AttributeType Type { get; private set; }
        public AttributeStatus Status { get; private set; }
        public int Version { get; private set; }

        public ICollection<AttributeOption> Options { get; private set; } = [];
        public ICollection<AttributeCategory> ApplicableCategories { get; private set; } = [];
    }
}
