using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AnisShop.Kafka.Sessions;

// One of these owns exactly one assigned partition, and nothing else ever touches that partition.
//
// A Service Bus session gives you an exclusive lock on *one session*, so ordering falls out for
// free. A Kafka partition gives you an exclusive lock on a *bundle of interleaved sessions*, so
// the session shape has to be rebuilt here:
//
//   1. the sender keys every message by its session id, so a session never spans two partitions
//      and its messages sit in the partition in production order;
//   2. this worker drains a batch and groups it by key — that regroups the bundle back into
//      session-shaped runs;
//   3. every run is handed to the handler concurrently with the other sessions' runs, but each
//      run is delivered strictly in arrival order, and no two calls for the same session ever
//      overlap.
//
// Note what is *not* here: no sorting, no deduplication, no sequence numbers, no gap detection.
// The order is the order the sender produced, exactly as a session receiver gives it to you.
// Anything beyond that is the consumer's business logic.
//
// Nothing is ever discarded. A message the handler cannot process blocks its partition until it
// can be, and the cursor never moves past it.
public sealed class PartitionSessionWorker
{
    private readonly TopicPartition _partition;
    private readonly KafkaSessionProcessorOptions _options;
    private readonly SemaphoreSlim _partitionSlots;
    private readonly SemaphoreSlim _sessionSlots;
    private readonly Func<ProcessSessionMessagesEventArgs, Task> _handler;
    private readonly Func<ProcessSessionErrorEventArgs, Task>? _errorHandler;
    private readonly Action<TopicPartitionOffset> _onPositionReady;
    private readonly ILogger _logger;

    private readonly Channel<ConsumeResult<string, byte[]>> _buffer;

    // The one holding set. A handler that threw gets exactly these messages again next cycle;
    // reading more cannot help, so the worker stops draining until they succeed.
    private readonly List<ConsumeResult<string, byte[]>> _failed = [];

    private CancellationTokenSource? _cts;
    private Task _worker = Task.CompletedTask;
    private long _storedPosition = OffsetWatermark.Unset;
    private long _highestDrainedOffset = OffsetWatermark.Unset;
    private int _consecutiveBlockedCycles;
    private bool _warnedAboutMissingKey;

    public PartitionSessionWorker(
        TopicPartition partition,
        KafkaSessionProcessorOptions options,
        SemaphoreSlim partitionSlots,
        SemaphoreSlim sessionSlots,
        Func<ProcessSessionMessagesEventArgs, Task> handler,
        Func<ProcessSessionErrorEventArgs, Task>? errorHandler,
        Action<TopicPartitionOffset> onPositionReady,
        ILogger logger)
    {
        _partition = partition;
        _options = options;
        _partitionSlots = partitionSlots;
        _sessionSlots = sessionSlots;
        _handler = handler;
        _errorHandler = errorHandler;
        _onPositionReady = onPositionReady;
        _logger = logger;

        _buffer = Channel.CreateBounded<ConsumeResult<string, byte[]>>(
            new BoundedChannelOptions(options.PartitionBufferSize)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
    }

    public TopicPartition Partition => _partition;

    private bool IsBlocked => _failed.Count > 0;

    public void Start(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _worker = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
    }

    // Non-blocking on purpose: the consume loop must never park on a slow partition. A false here
    // is the signal to pause this partition on the broker until the buffer drains.
    public bool TryEnqueue(ConsumeResult<string, byte[]> result) => _buffer.Writer.TryWrite(result);

    public async Task StopAsync()
    {
        _buffer.Writer.TryComplete();

        if (_cts is not null)
            await _cts.CancelAsync();

        try
        {
            await _worker;
        }
        catch (OperationCanceledException)
        {
            // Expected on revoke/shutdown: whatever was mid-flight is simply not committed.
        }
        finally
        {
            _cts?.Dispose();
            _cts = null;
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await WaitForWorkAsync(cancellationToken))
            {
                var batch = await BuildBatchAsync(cancellationToken);

                if (batch.Count > 0)
                    await HandleBatchAsync(batch, cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                    return;

                AdvancePosition();
                ReportBlockage();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down or losing the partition.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Partition worker for {Partition} stopped unexpectedly", _partition);
        }
    }

    private async ValueTask<bool> WaitForWorkAsync(CancellationToken cancellationToken)
    {
        // Blocked: nothing that arrives can help, so wait out the backoff and retry exactly what
        // failed. The buffer fills meanwhile and the processor pauses the partition on the broker.
        if (IsBlocked)
        {
            await Task.Delay(BlockedBackoff(), cancellationToken);
            return true;
        }

        return await _buffer.Reader.WaitToReadAsync(cancellationToken);
    }

    private async Task<List<ConsumeResult<string, byte[]>>> BuildBatchAsync(CancellationToken cancellationToken)
    {
        var wasBlocked = IsBlocked;

        var batch = new List<ConsumeResult<string, byte[]>>(_failed);
        _failed.Clear();

        // Reading more cannot clear a failure, and every message consumed past one is a message
        // that will have to be read again anyway — the cursor is pinned below it.
        if (wasBlocked)
            return batch;

        var drained = DrainBuffer(batch);

        // Linger briefly so a burst lands in one pass. Fatter batches mean more distinct sessions
        // per pass, which is exactly what widens the per-partition fan-out.
        if (drained > 0 && batch.Count < _options.PartitionBufferSize && _options.BatchLingerMilliseconds > 0)
        {
            await Task.Delay(_options.BatchLingerMilliseconds, cancellationToken);
            DrainBuffer(batch);
        }

        return batch;
    }

    private int DrainBuffer(List<ConsumeResult<string, byte[]>> batch)
    {
        var drained = 0;

        while (batch.Count < _options.PartitionBufferSize && _buffer.Reader.TryRead(out var result))
        {
            drained++;
            _highestDrainedOffset = Math.Max(_highestDrainedOffset, result.Offset.Value);
            batch.Add(result);
        }

        return drained;
    }

    private async Task HandleBatchAsync(
        List<ConsumeResult<string, byte[]>> batch,
        CancellationToken cancellationToken)
    {
        // Only so many partitions may be doing this at once. Acquired here rather than around the
        // wait, so an idle partition never holds a slot.
        await _partitionSlots.WaitAsync(cancellationToken);

        try
        {
            // The regrouping step: one partition's interleaved bundle becomes N session-shaped
            // runs. GroupBy preserves source order inside each group, and the batch was drained
            // in offset order, so every run is in production order.
            var sessions = batch
                .GroupBy(SessionIdOf)
                .Select(group => (SessionId: group.Key, Messages: group.ToList()))
                .ToList();

            var outcomes = await Task.WhenAll(
                sessions.Select(session => HandleSessionAsync(session.SessionId, session.Messages, cancellationToken)));

            if (cancellationToken.IsCancellationRequested)
                return;

            foreach (var unhandled in outcomes)
                _failed.AddRange(unhandled);

            _logger.LogDebug(
                "Handled {Sessions} session(s) ({Messages} messages) from {Partition}; {Failed} failed",
                sessions.Count, batch.Count, _partition, _failed.Count);
        }
        finally
        {
            _partitionSlots.Release();
        }
    }

    // Never throws (except on shutdown): one failing session must not take down its neighbours'
    // results. Returns the messages it did not get through, which become the retry set.
    private async Task<IReadOnlyList<ConsumeResult<string, byte[]>>> HandleSessionAsync(
        string sessionId,
        List<ConsumeResult<string, byte[]>> messages,
        CancellationToken cancellationToken)
    {
        await _sessionSlots.WaitAsync(cancellationToken);

        try
        {
            var handled = 0;

            // A session holding more than MaxMessagesPerSession is delivered in several calls,
            // back to back and still in order — never concurrently with itself.
            foreach (var chunk in messages.Chunk(_options.MaxMessagesPerSession))
            {
                try
                {
                    await InvokeHandlerAsync(sessionId, chunk, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(
                        ex, "Handler failed for session {SessionId} on {Partition}; the partition will hold until it succeeds",
                        sessionId, _partition);

                    await RaiseErrorAsync(ex, sessionId, cancellationToken);

                    // Everything from the failing call onwards is retried; the calls that already
                    // returned are done, so they never run twice.
                    return messages.GetRange(handled, messages.Count - handled);
                }

                handled += chunk.Length;
            }

            return [];
        }
        finally
        {
            _sessionSlots.Release();
        }
    }

    private async Task InvokeHandlerAsync(
        string sessionId,
        ConsumeResult<string, byte[]>[] chunk,
        CancellationToken cancellationToken)
    {
        // Bound a single call so a wedged handler surfaces as a retryable failure with a log line,
        // instead of silently holding the batch open forever.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.HandlerTimeoutMilliseconds);

        await _handler(new ProcessSessionMessagesEventArgs(sessionId, chunk, _partition, timeout.Token));
    }

    private async Task RaiseErrorAsync(Exception exception, string sessionId, CancellationToken cancellationToken)
    {
        if (_errorHandler is null)
            return;

        try
        {
            await _errorHandler(new ProcessSessionErrorEventArgs(exception, sessionId, _partition, cancellationToken));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "The error handler itself threw for {Partition}", _partition);
        }
    }

    // A message with no key was never assigned to a session by the sender, and Kafka spreads such
    // messages across partitions with no ordering at all. Grouping them under one empty session is
    // the conservative reading — they stay ordered relative to each other and nothing is lost —
    // but it is a sender bug, so say so once.
    private string SessionIdOf(ConsumeResult<string, byte[]> result)
    {
        if (result.Message.Key is not null)
            return result.Message.Key;

        if (!_warnedAboutMissingKey)
        {
            _warnedAboutMissingKey = true;
            _logger.LogWarning(
                "{Partition} carries messages with no key. They have no session and no ordering guarantee; "
                + "they are handled as one session. The sender should key every message.",
                _partition);
        }

        return string.Empty;
    }

    private void AdvancePosition()
    {
        if (_highestDrainedOffset == OffsetWatermark.Unset)
            return;

        long? lowestPending = null;

        foreach (var message in _failed)
            lowestPending = Math.Min(lowestPending ?? long.MaxValue, message.Offset.Value);

        if (!OffsetWatermark.TryAdvance(_highestDrainedOffset, lowestPending, _storedPosition, out var nextPosition))
            return;

        _storedPosition = nextPosition;
        _onPositionReady(new TopicPartitionOffset(_partition, new Offset(nextPosition)));
    }

    private void ReportBlockage()
    {
        if (!IsBlocked)
        {
            if (_consecutiveBlockedCycles > 0)
            {
                _logger.LogInformation(
                    "{Partition} is unblocked after {Cycles} cycle(s) and is consuming again",
                    _partition, _consecutiveBlockedCycles);
            }

            _consecutiveBlockedCycles = 0;
            return;
        }

        _consecutiveBlockedCycles++;

        var offset = _failed.Min(message => message.Offset.Value);

        // Loud once, then steady — the escalating backoff already throttles this to a couple of
        // lines a minute at the cap.
        if (_consecutiveBlockedCycles == 1)
        {
            _logger.LogCritical(
                "{Partition} is blocked at offset {Offset} ({Failed} message(s) failing). "
                + "Nothing past it will be consumed or committed until it succeeds.",
                _partition, offset, _failed.Count);
        }
        else
        {
            _logger.LogWarning(
                "{Partition} still blocked at offset {Offset} after {Cycles} cycle(s)",
                _partition, offset, _consecutiveBlockedCycles);
        }
    }

    private TimeSpan BlockedBackoff()
    {
        var exponent = Math.Min(_consecutiveBlockedCycles, 20);
        var delay = _options.RetryBackoffMilliseconds * Math.Pow(2, exponent);

        return TimeSpan.FromMilliseconds(Math.Min(delay, _options.MaxRetryBackoffMilliseconds));
    }
}
