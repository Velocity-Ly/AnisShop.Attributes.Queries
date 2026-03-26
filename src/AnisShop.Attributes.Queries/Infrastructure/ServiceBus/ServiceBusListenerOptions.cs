namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public class ServiceBusListenerOptions
{
    public const string SectionName = "ServiceBus";

    public required string TopicName { get; init; }
    public required string SubscriptionName { get; init; }
    public int MaxConcurrentSessions { get; init; } = 1000;
    public int MaxMessagesPerSession { get; init; } = 100;
    public bool EnableDeadLetterQueue { get; init; }
}
