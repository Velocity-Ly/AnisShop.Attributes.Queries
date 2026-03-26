using AnisShop.Attributes.Queries.Features.EventsHandler;
using Azure.Messaging.ServiceBus;
using Mediator;
using Microsoft.Extensions.Options;

namespace AnisShop.Attributes.Queries.Infrastructure.ServiceBus;

public class ServiceBusEventListener : IHostedService, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ServiceBusListenerOptions _options;
    private readonly IEventDeserializer _deserializer;
    private readonly EventBatchProcessor _batchProcessor;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ServiceBusEventListener> _logger;
    private readonly SemaphoreSlim _sessionSemaphore;

    private CancellationTokenSource? _cts;
    private Task? _sessionLoopTask;
    private ServiceBusProcessor? _dlqProcessor;

    public ServiceBusEventListener(
        ServiceBusClient client,
        IOptions<ServiceBusListenerOptions> options,
        IEventDeserializer deserializer,
        EventBatchProcessor batchProcessor,
        IServiceScopeFactory scopeFactory,
        ILogger<ServiceBusEventListener> logger)
    {
        _client = client;
        _options = options.Value;
        _deserializer = deserializer;
        _batchProcessor = batchProcessor;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _sessionSemaphore = new SemaphoreSlim(_options.MaxConcurrentSessions);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _sessionLoopTask = RunSessionLoopAsync(_cts.Token);
        await StartDlqProcessorAsync(_cts.Token);

        _logger.LogInformation(
            "Service Bus listener started for {Topic}/{Subscription} with max {MaxSessions} concurrent sessions",
            _options.TopicName, _options.SubscriptionName, _options.MaxConcurrentSessions);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts is not null)
            await _cts.CancelAsync();

        if (_dlqProcessor is not null)
            await _dlqProcessor.StopProcessingAsync(cancellationToken);

        if (_sessionLoopTask is not null)
            await _sessionLoopTask;

        _logger.LogInformation("Service Bus listener stopped");
    }

    public async ValueTask DisposeAsync()
    {
        if (_dlqProcessor is not null)
            await _dlqProcessor.DisposeAsync();

        _cts?.Dispose();
        _sessionSemaphore.Dispose();
    }

    private async Task RunSessionLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await _sessionSemaphore.WaitAsync(ct);
                _ = ProcessNextSessionAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in session loop");
            }
        }
    }

    private async Task ProcessNextSessionAsync(CancellationToken ct)
    {
        ServiceBusSessionReceiver? receiver = null;
        try
        {
            receiver = await _client.AcceptNextSessionAsync(
                _options.TopicName,
                _options.SubscriptionName,
                new ServiceBusSessionReceiverOptions { PrefetchCount = _options.MaxMessagesPerSession },
                ct);

            var messages = await receiver.ReceiveMessagesAsync(
                maxMessages: _options.MaxMessagesPerSession,
                maxWaitTime: TimeSpan.FromSeconds(5),
                cancellationToken: ct);

            if (messages.Count == 0)
                return;

            var deserialized = new List<(ServiceBusReceivedMessage Message, Events.EventBase Event)>();

            foreach (var message in messages)
            {
                var evt = _deserializer.Deserialize(message);
                if (evt is null)
                {
                    await receiver.DeadLetterMessageAsync(message, "UnknownEventType",
                        "Could not deserialize event", cancellationToken: ct);
                    continue;
                }

                deserialized.Add((message, evt));
            }

            if (deserialized.Count == 0)
                return;

            var result = _batchProcessor.Process(deserialized);

            foreach (var duplicate in result.Duplicates)
                await receiver.CompleteMessageAsync(duplicate, ct);

            using var scope = _scopeFactory.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            var incomingEvents = new IncomingEvents
            {
                Events = result.Contiguous.Select(x => x.Event).ToList()
            };

            var success = await mediator.Send(incomingEvents, ct);

            if (success)
            {
                foreach (var (message, _) in result.Contiguous)
                    await receiver.CompleteMessageAsync(message, ct);

                _logger.LogDebug(
                    "Completed {Count} events for session {SessionId}, versions {Min}-{Max}",
                    result.Contiguous.Count,
                    receiver.SessionId,
                    result.Contiguous[0].Event.Version,
                    result.Contiguous[^1].Event.Version);
            }
            else
            {
                _logger.LogWarning(
                    "Handler returned false for session {SessionId}, {Count} events not completed",
                    receiver.SessionId, result.Contiguous.Count);
            }
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.ServiceTimeout)
        {
            _logger.LogWarning("No sessions available, backing off for 10s");
            await Task.Delay(TimeSpan.FromSeconds(10), ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutting down
            _logger.LogError("Session processing cancelled due to shutdown");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing session");
        }
        finally
        {
            if (receiver is not null)
                await receiver.DisposeAsync();

            _sessionSemaphore.Release();
        }
    }

    private async Task StartDlqProcessorAsync(CancellationToken ct)
    {
        if (!_options.EnableDeadLetterQueue)
        {
            _logger.LogInformation("Dead letter queue processing is disabled");
            return;
        }

        _dlqProcessor = _client.CreateProcessor(
            _options.TopicName,
            _options.SubscriptionName,
            new ServiceBusProcessorOptions
            {
                SubQueue = SubQueue.DeadLetter,
                MaxConcurrentCalls = 10,
                AutoCompleteMessages = false,
            });

        _dlqProcessor.ProcessMessageAsync += async args =>
        {
            var evt = _deserializer.Deserialize(args.Message);
            if (evt is null)
            {
                await args.CompleteMessageAsync(args.Message);
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            var success = await mediator.Send(
                new IncomingEvents { Events = [evt] },
                args.CancellationToken);

            if (success)
                await args.CompleteMessageAsync(args.Message);
            else
            {
                await Task.Delay(TimeSpan.FromSeconds(5), args.CancellationToken);
                await args.AbandonMessageAsync(args.Message);
            }
        };

        _dlqProcessor.ProcessErrorAsync += args =>
        {
            _logger.LogError(args.Exception, "Dead letter queue processor error");
            return Task.CompletedTask;
        };

        await _dlqProcessor.StartProcessingAsync(ct);
    }
}
