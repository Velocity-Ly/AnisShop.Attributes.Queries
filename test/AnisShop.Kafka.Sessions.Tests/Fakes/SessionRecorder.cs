using System.Collections.Concurrent;
using System.Text;

namespace AnisShop.Kafka.Sessions.Tests.Fakes
{
    // Stands in for the consuming application's handler. Records every call so tests can assert on
    // what was delivered, in what order, and in how many calls.
    public sealed class SessionRecorder
    {
        private static readonly TimeSpan RendezvousTimeout = TimeSpan.FromSeconds(5);

        private readonly ConcurrentQueue<(string SessionId, string[] Payloads)> _calls = new();
        private readonly ConcurrentDictionary<string, byte> _failing = new();
        private readonly ConcurrentDictionary<string, int> _inFlight = new();
        private readonly ConcurrentQueue<ProcessSessionErrorEventArgs> _errors = new();
        private readonly TaskCompletionSource _rendezvousReached =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _rendezvousTarget;
        private int _rendezvousArrived;
        private int _sameSessionOverlaps;

        // Makes a bug in which one session's calls run in parallel actually observable.
        public TimeSpan CallDelay { get; set; } = TimeSpan.Zero;

        public IReadOnlyList<(string SessionId, string[] Payloads)> Calls => [.. _calls];

        public IReadOnlyList<(string SessionId, string[] Payloads)> CallsFor(string sessionId) =>
            [.. _calls.Where(call => call.SessionId == sessionId)];

        // Everything delivered for one session, flattened, in the order it was delivered.
        public IReadOnlyList<string> PayloadsFor(string sessionId) =>
            [.. _calls.Where(call => call.SessionId == sessionId).SelectMany(call => call.Payloads)];

        public bool HasSession(string sessionId) => _calls.Any(call => call.SessionId == sessionId);

        public IReadOnlyList<ProcessSessionErrorEventArgs> Errors => [.. _errors];

        // The invariant a session receiver gives you for free: two calls for the same session must
        // never be in flight at once.
        public bool SawSameSessionOverlap => Volatile.Read(ref _sameSessionOverlaps) > 0;

        public void FailSession(string sessionId) => _failing[sessionId] = 0;

        public void HealSession(string sessionId) => _failing.TryRemove(sessionId, out _);

        // Holds every arriving call until `sessions` of them are inside at once. If the fan-out were
        // serialised this never completes, so the test fails on the flag rather than a timing guess.
        public void ExpectConcurrent(int sessions) => Volatile.Write(ref _rendezvousTarget, sessions);

        public bool ConcurrencyReached => _rendezvousReached.Task.IsCompletedSuccessfully;

        public async Task HandleAsync(ProcessSessionMessagesEventArgs args)
        {
            if (_inFlight.AddOrUpdate(args.SessionId, 1, (_, count) => count + 1) > 1)
                Interlocked.Increment(ref _sameSessionOverlaps);

            try
            {
                if (_failing.ContainsKey(args.SessionId))
                    throw new InvalidOperationException($"Handler for session {args.SessionId} is failing");

                await RendezvousAsync();

                if (CallDelay > TimeSpan.Zero)
                    await Task.Delay(CallDelay);

                _calls.Enqueue((
                    args.SessionId,
                    [.. args.Messages.Select(message => Encoding.UTF8.GetString(message.Message.Value))]));
            }
            finally
            {
                _inFlight.AddOrUpdate(args.SessionId, 0, (_, count) => count - 1);
            }
        }

        public Task OnErrorAsync(ProcessSessionErrorEventArgs args)
        {
            _errors.Enqueue(args);

            return Task.CompletedTask;
        }

        private async Task RendezvousAsync()
        {
            if (Volatile.Read(ref _rendezvousTarget) == 0)
                return;

            if (Interlocked.Increment(ref _rendezvousArrived) >= Volatile.Read(ref _rendezvousTarget))
                _rendezvousReached.TrySetResult();

            await Task.WhenAny(_rendezvousReached.Task, Task.Delay(RendezvousTimeout));
        }
    }
}
