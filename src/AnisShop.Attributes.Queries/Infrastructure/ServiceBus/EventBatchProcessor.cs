using AnisShop.Attributes.Queries.Events;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public record EventBatchResult<TMessage>(
    IReadOnlyList<(TMessage Message, EventBase Event)> Contiguous,
    IReadOnlyList<TMessage> Duplicates);

public class EventBatchProcessor
{
    public EventBatchResult<TMessage> Process<TMessage>(
        List<(TMessage Message, EventBase Event)> items)
    {
        if (items.Count == 0)
            return new EventBatchResult<TMessage>([], []);

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

        return new EventBatchResult<TMessage>(contiguous, duplicates);
    }
}
