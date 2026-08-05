using AnisShop.Attributes.Queries.Events;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

// Held: the tail that sits *behind a version gap*. Service Bus simply leaves those messages
// uncompleted and lets redelivery bring them back on the next session lock.
public record EventBatchResult<TMessage>(
    IReadOnlyList<(TMessage Message, EventBase Event)> Contiguous,
    IReadOnlyList<TMessage> Duplicates,
    IReadOnlyList<(TMessage Message, EventBase Event)> Held);

// Given one session's messages, decide which prefix is safe to project. The Kafka path solves the
// same problem, but it has to do it per aggregate carved out of a partition batch, so that copy
// lives in the AnisShop.Kafka.OrderedStreams package rather than being shared from here.
public class EventBatchProcessor
{
    public EventBatchResult<TMessage> Process<TMessage>(
        List<(TMessage Message, EventBase Event)> items)
    {
        if (items.Count == 0)
            return new EventBatchResult<TMessage>([], [], []);

        items.Sort((a, b) => a.Event.Version.CompareTo(b.Event.Version));

        var unique = new List<(TMessage Message, EventBase Event)>();
        var duplicates = new List<TMessage>();

        foreach (var item in items)
        {
            if (unique.Count > 0 && unique[^1].Event.Version == item.Event.Version)
                duplicates.Add(item.Message);
            else
                unique.Add(item);
        }

        var contiguous = new List<(TMessage Message, EventBase Event)> { unique[0] };

        for (var i = 1; i < unique.Count; i++)
        {
            if (unique[i].Event.Version == unique[i - 1].Event.Version + 1)
                contiguous.Add(unique[i]);
            else
                break;
        }

        var held = unique.Count > contiguous.Count
            ? unique.GetRange(contiguous.Count, unique.Count - contiguous.Count)
            : [];

        return new EventBatchResult<TMessage>(contiguous, duplicates, held);
    }
}
