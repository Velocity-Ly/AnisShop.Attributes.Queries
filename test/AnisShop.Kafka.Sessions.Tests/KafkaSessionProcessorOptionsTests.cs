using System.ComponentModel.DataAnnotations;

namespace AnisShop.Kafka.Sessions.Tests
{
    public class KafkaSessionProcessorOptionsTests
    {
        [Theory]
        [InlineData(8, 8)]
        [InlineData(8, 1000)]
        public void Validate_EnoughSessionSlotsForEveryPartition_Passes(int partitions, int sessions)
        {
            // Arrange
            var options = Options(partitions, sessions);

            // Act
            var results = Validate(options);

            // Assert
            Assert.Empty(results);
        }

        [Fact]
        public void Validate_FewerSessionSlotsThanPartitions_Fails()
        {
            // Arrange: a partition that grabs a slot and then cannot get a single session slot
            // holds the slot while doing nothing, so partition concurrency silently drops.
            var options = Options(partitions: 32, sessions: 8);

            // Act
            var results = Validate(options);

            // Assert
            var failure = Assert.Single(results);
            Assert.Contains(nameof(KafkaSessionProcessorOptions.MaxConcurrentSessions), failure.MemberNames);
            Assert.Contains(nameof(KafkaSessionProcessorOptions.MaxConcurrentPartitions), failure.MemberNames);
        }

        [Fact]
        public void Validate_MissingRequiredConnectionDetails_Fails()
        {
            // Arrange: appsettings ships empty placeholders, filled from secrets at runtime — so
            // this must fail when the options are resolved rather than pass silently.
            var options = new KafkaSessionProcessorOptions
            {
                BootstrapServers = string.Empty,
                Topic = string.Empty,
                ConsumerGroup = string.Empty,
            };

            // Act
            var results = Validate(options);

            // Assert
            Assert.Equal(3, results.Count);
        }

        private static KafkaSessionProcessorOptions Options(int partitions, int sessions) =>
            new()
            {
                BootstrapServers = "broker:9092",
                Topic = "messages",
                ConsumerGroup = "group",
                MaxConcurrentPartitions = partitions,
                MaxConcurrentSessions = sessions,
            };

        private static IReadOnlyList<ValidationResult> Validate(KafkaSessionProcessorOptions options)
        {
            var results = new List<ValidationResult>();

            Validator.TryValidateObject(options, new ValidationContext(options), results, validateAllProperties: true);

            return results;
        }
    }
}
