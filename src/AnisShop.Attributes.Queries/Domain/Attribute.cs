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
            AttributeScope scope,
            AttributeStatus status,
            int version)
        {
            Id = id;
            ArabicDisplayName = arabicDisplayName;
            EnglishDisplayName = englishDisplayName;
            ArabicDescription = arabicDescription;
            EnglishDescription = englishDescription;
            Type = type;
            Scope = scope;
            Status = status;
            Version = version;
        }

        public Guid Id { get; private set; }
        public string ArabicDisplayName { get; private set; }
        public string EnglishDisplayName { get; private set; }
        public string? ArabicDescription { get; private set; }
        public string? EnglishDescription { get; private set; }
        public AttributeType Type { get; private set; }
        public AttributeScope Scope { get; private set; }
        public AttributeStatus Status { get; private set; }
        public string? ArabicDeprecationWarning { get; private set; }
        public string? EnglishDeprecationWarning { get; private set; }
        public string? ArabicDisableReason { get; private set; }
        public string? EnglishDisableReason { get; private set; }
        public int Version { get; private set; }

        public ICollection<AttributeOption> Options { get; private set; } = [];
        public ICollection<AttributeTarget> ApplicableTargets { get; private set; } = [];

        public static Attribute Create(
            Guid id,
            string arabicDisplayName,
            string englishDisplayName,
            string? arabicDescription,
            string? englishDescription,
            AttributeType type,
            AttributeScope scope,
            int version)
            => new(
                id,
                arabicDisplayName,
                englishDisplayName,
                arabicDescription,
                englishDescription,
                type,
                scope,
                AttributeStatus.Draft,
                version);

        public void Publish(int version)
        {
            Status = AttributeStatus.Published;
            Version = version;
        }

        public void ChangeMetadata(
            string arabicDisplayName,
            string englishDisplayName,
            string? arabicDescription,
            string? englishDescription,
            int version)
        {
            ArabicDisplayName = arabicDisplayName;
            EnglishDisplayName = englishDisplayName;
            ArabicDescription = arabicDescription;
            EnglishDescription = englishDescription;
            Version = version;
        }

        public void ChangeType(AttributeType type, int version)
        {
            Type = type;
            Version = version;
        }

        public void MarkAsDeprecated(string arabicWarning, string englishWarning, int version)
        {
            Status = AttributeStatus.Deprecated;
            ArabicDeprecationWarning = arabicWarning;
            EnglishDeprecationWarning = englishWarning;
            Version = version;
        }

        public void RemoveDeprecationWarning(int version)
        {
            Status = AttributeStatus.Published;
            ArabicDeprecationWarning = null;
            EnglishDeprecationWarning = null;
            Version = version;
        }

        public void Disable(string arabicReason, string englishReason, int version)
        {
            Status = AttributeStatus.Disabled;
            ArabicDisableReason = arabicReason;
            EnglishDisableReason = englishReason;
            Version = version;
        }

        public void AddTargets(IEnumerable<int> targetIds, int version)
        {
            foreach (var targetId in targetIds)
            {
                if (ApplicableTargets.Any(t => t.TargetId == targetId))
                    continue;

                ApplicableTargets.Add(AttributeTarget.Create(Id, targetId));
            }

            Version = version;
        }

        public void RemoveTargets(IEnumerable<int> targetIds, int version)
        {
            var ids = targetIds.ToHashSet();
            var toRemove = ApplicableTargets.Where(t => ids.Contains(t.TargetId)).ToList();

            foreach (var target in toRemove)
                ApplicableTargets.Remove(target);

            Version = version;
        }

        public void AddOption(string key, string arabicLabel, string englishLabel, int version)
        {
            if (!Options.Any(o => o.Key == key))
            {
                // Append to the bottom: MAX(existing SortOrder) + 1 (0 when this is the first).
                var sortOrder = Options.Count == 0 ? 0 : Options.Max(o => o.SortOrder) + 1;
                Options.Add(AttributeOption.Create(Id, key, arabicLabel, englishLabel, sortOrder));
            }

            Version = version;
        }

        public void ChangeOptionLabel(string key, string arabicLabel, string englishLabel, int version)
        {
            var option = Options.FirstOrDefault(o => o.Key == key);
            option?.ChangeLabel(arabicLabel, englishLabel);

            Version = version;
        }

        public void DisableOption(string key, int version)
        {
            var option = Options.FirstOrDefault(o => o.Key == key);
            option?.Disable();

            Version = version;
        }

        public void RemoveOption(string key, int version)
        {
            var option = Options.FirstOrDefault(o => o.Key == key);
            if (option is not null)
                Options.Remove(option);

            Version = version;
        }

        public void ReorderOptions(IReadOnlyList<string> orderedKeys, int version)
        {
            // The position of each key in the array IS its SortOrder (0-based).
            for (var index = 0; index < orderedKeys.Count; index++)
            {
                var option = Options.FirstOrDefault(o => o.Key == orderedKeys[index]);
                option?.SetSortOrder(index);
            }

            Version = version;
        }
    }
}
