using System.ComponentModel.DataAnnotations;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public class ServiceBusListenerOptions
{
    public const string SectionName = "ServiceBus";

    [Required]
    public required string TopicName { get; init; }

    [Required]
    public required string SubscriptionName { get; init; }

    [Range(1, int.MaxValue)]
    public int MaxConcurrentSessions { get; init; } = 1000;

    [Range(1, int.MaxValue)]
    public int MaxMessagesPerSession { get; init; } = 100;

    public bool EnableDeadLetterQueue { get; init; }
}
