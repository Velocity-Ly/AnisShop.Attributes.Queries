using AnisShop.Kafka.Sessions.Tests.Fakes;

namespace AnisShop.Kafka.Sessions.Tests
{
    // The behaviours a Service Bus session gives you for free, reconstructed on top of a partition
    // that carries many interleaved sessions. Note what is never asserted here: nothing about
    // sequence numbers, sorting or gaps. The order is the sender's order, full stop.
    public class PartitionSessionWorkerTests
    {
        [Fact]
        public async Task Handle_InterleavedSessions_DeliversEachSessionInSenderOrder()
        {
            // Arrange: three senders writing concurrently, so the partition holds
            // A1, B1, C1, A2, B2, C2, ... — the shape a session receiver never has to deal with.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);

            // Act
            harness.Enqueue(log.AppendInterleaved(
                ("A", ["A1", "A2", "A3"]),
                ("B", ["B1", "B2"]),
                ("C", ["C1", "C2", "C3"])));

            await harness.WaitForPosition(log.NextOffset);

            // Assert: each session came back in the order its sender produced it, not in the order
            // the partition happened to hold it
            Assert.Equal(["A1", "A2", "A3"], harness.Recorder.PayloadsFor("A"));
            Assert.Equal(["B1", "B2"], harness.Recorder.PayloadsFor("B"));
            Assert.Equal(["C1", "C2", "C3"], harness.Recorder.PayloadsFor("C"));
        }

        [Fact]
        public async Task Handle_ManySessionsInOneBatch_DeliversAllOfThem()
        {
            // Arrange: 50 sessions sharing one partition. On the Service Bus side these would be 50
            // session receivers; here they are 50 groups inside a single batch.
            var sessions = Enumerable.Range(0, 50)
                .Select(index => ($"session-{index}", new[] { $"{index}-first", $"{index}-second" }))
                .ToArray();

            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);

            // Act
            harness.Enqueue(log.AppendInterleaved(sessions));
            await harness.WaitForPosition(log.NextOffset);

            // Assert
            foreach (var (sessionId, payloads) in sessions)
                Assert.Equal(payloads, harness.Recorder.PayloadsFor(sessionId));
        }

        [Fact]
        public async Task Handle_SessionsInOneBatch_RunConcurrently()
        {
            // Arrange: the whole reason the package exists. Every call blocks inside the handler
            // until five are in flight at once, so a serialised fan-out could never complete — no
            // timing guess involved.
            const int Sessions = 5;

            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log, startImmediately: false);
            harness.Recorder.ExpectConcurrent(Sessions);

            harness.Enqueue(log.AppendInterleaved(
                [.. Enumerable.Range(0, Sessions).Select(index => ($"session-{index}", new[] { $"{index}" }))]));

            // Act: the buffer is already full, so the first batch carries all five sessions
            harness.Start();
            await harness.WaitForPosition(log.NextOffset);

            // Assert
            Assert.True(harness.Recorder.ConcurrencyReached,
                $"{Sessions} sessions were never handled at the same time");
        }

        [Fact]
        public async Task Handle_OneSession_NeverOverlapsWithItself()
        {
            // Arrange: the other half of the session guarantee. A session split across several
            // calls must be delivered one call at a time — parallelism is *between* sessions only.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(
                log, maxMessagesPerSession: 2, startImmediately: false);

            // Slow enough that overlapping calls would be caught red-handed.
            harness.Recorder.CallDelay = TimeSpan.FromMilliseconds(20);

            harness.Enqueue(log.Append("A", "A1", "A2", "A3", "A4", "A5", "A6"));
            harness.Enqueue(log.Append("B", "B1", "B2", "B3", "B4"));

            // Act
            harness.Start();
            await harness.WaitForPosition(log.NextOffset);

            // Assert
            Assert.False(harness.Recorder.SawSameSessionOverlap,
                "two calls for the same session were in flight at once");
            Assert.Equal(["A1", "A2", "A3", "A4", "A5", "A6"], harness.Recorder.PayloadsFor("A"));
            Assert.Equal(["B1", "B2", "B3", "B4"], harness.Recorder.PayloadsFor("B"));
        }

        [Fact]
        public async Task Handle_SessionLargerThanMaxMessagesPerSession_DeliversInSeveralOrderedCalls()
        {
            // Arrange: the counterpart of ServiceBusSessionProcessor.MaxMessagesPerSession. A big
            // session arrives in several back-to-back calls rather than one huge one.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(
                log, maxMessagesPerSession: 2, startImmediately: false);

            harness.Enqueue(log.Append("A", "A1", "A2", "A3", "A4", "A5"));

            // Act
            harness.Start();
            await harness.WaitForPosition(log.NextOffset);

            // Assert: chunked at the cap, in order, with the remainder in a final short call
            var calls = harness.Recorder.CallsFor("A");
            Assert.Equal(3, calls.Count);
            Assert.Equal(["A1", "A2"], calls[0].Payloads);
            Assert.Equal(["A3", "A4"], calls[1].Payloads);
            Assert.Equal(["A5"], calls[2].Payloads);
        }

        [Fact]
        public async Task Handle_MessagesWithNoKey_AreDeliveredAsOneSession()
        {
            // Arrange: a sender that forgot the key. Kafka gives those messages no session at all,
            // so the conservative reading is one shared ordered session — never discarded, never
            // silently parallelised.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);

            // Act
            harness.Enqueue(log.AppendWithoutKey("first"), log.AppendWithoutKey("second"));
            await harness.WaitForPosition(log.NextOffset);

            // Assert
            Assert.Equal(["first", "second"], harness.Recorder.PayloadsFor(string.Empty));
        }

        [Fact]
        public async Task Handle_HandlerThrows_BlocksThePartitionAndStopsConsuming()
        {
            // Arrange: throwing is the only failure signal there is. Nothing is ever discarded, so
            // it stops the partition dead — including for messages behind it that would have been
            // perfectly fine.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);
            harness.Recorder.FailSession("boom");

            // Act: the failing message establishes the blockage...
            harness.Enqueue(log.Append("boom", "first"));
            await harness.WaitUntil(
                () => harness.StoredPosition == 0, "the partition to block at offset 0");
            await harness.Settle();

            // ...then a good message is offered behind it
            harness.Enqueue(log.Append("behind", "fine"));
            await harness.Settle();

            // Assert: it is not consumed, and the cursor still points at the failing message, so a
            // restart re-reads from there and loses nothing
            Assert.Equal(0, harness.StoredPosition);
            Assert.False(harness.Recorder.HasSession("behind"));
        }

        [Fact]
        public async Task Handle_HandlerRecovers_DrainsTheBacklogInOrder()
        {
            // Arrange: the failure that actually happens — a database briefly down. Blocking is
            // only acceptable if it unblocks itself, with the backlog intact and still in order.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);
            harness.Recorder.FailSession("flaky");

            harness.Enqueue(log.Append("flaky", "one", "two"));
            await harness.WaitUntil(
                () => harness.StoredPosition == 0, "the partition to block at offset 0");
            harness.Enqueue(log.Append("behind", "fine"));
            await harness.Settle();

            Assert.False(harness.Recorder.HasSession("behind"));

            // Act: the database comes back
            harness.Recorder.HealSession("flaky");
            await harness.WaitForPosition(log.NextOffset);

            // Assert
            Assert.Equal(["one", "two"], harness.Recorder.PayloadsFor("flaky"));
            Assert.Equal(["fine"], harness.Recorder.PayloadsFor("behind"));
        }

        [Fact]
        public async Task Handle_HandlerThrows_RaisesTheErrorEvent()
        {
            // Arrange: failures have to be observable, not just logged.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);
            harness.Recorder.FailSession("boom");

            // Act
            harness.Enqueue(log.Append("boom", "first"));
            await harness.WaitUntil(
                () => harness.Recorder.Errors.Count > 0, "the error handler to be called");

            // Assert
            var error = harness.Recorder.Errors[0];
            Assert.Equal("boom", error.SessionId);
            Assert.Equal(log.TopicPartition, error.Partition);
            Assert.IsType<InvalidOperationException>(error.Exception);
        }

        [Fact]
        public async Task Handle_FullBuffer_StopsAcceptingRecords()
        {
            // Arrange: a tiny buffer so backpressure is reachable. A blocked worker stops draining,
            // so the buffer fills and the processor pauses the partition on the broker instead of
            // buffering without limit — and it must do that without spinning.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log, partitionBufferSize: 2);
            harness.Recorder.FailSession("stuck");

            // Act
            harness.Enqueue(log.Append("stuck", "one"));
            await harness.WaitUntil(
                () => harness.StoredPosition == 0, "the partition to block at offset 0");

            var refused = false;
            await harness.WaitUntil(
                () =>
                {
                    if (harness.TryEnqueue(log.Append("behind", "fine")))
                        return false;

                    refused = true;
                    return true;
                },
                "the buffer to stop accepting records");

            // Assert: backpressure engaged, and the cursor never moved past the failing message
            Assert.True(refused);
            Assert.Equal(0, harness.StoredPosition);
        }

        [Fact]
        public async Task Handle_RedeliveredMessages_AreHandedOverAgain()
        {
            // Arrange: the cursor is only ever stored below the oldest unfinished message, so a
            // restart or rebalance re-reads a tail that was already handled. The package does not
            // and cannot deduplicate — that is exactly why the handler must be idempotent, and this
            // pins the behaviour so nobody assumes otherwise.
            var log = new PartitionLog();
            await using var harness = new PartitionWorkerHarness(log);

            harness.Enqueue(log.Append("A", "A1", "A2"));
            await harness.WaitForPosition(log.NextOffset);

            // Act: the same messages again at fresh offsets
            harness.Enqueue(log.Append("A", "A1", "A2"));
            await harness.WaitForPosition(log.NextOffset);

            // Assert: delivered twice, in order, with no transport-level deduplication
            Assert.Equal(["A1", "A2", "A1", "A2"], harness.Recorder.PayloadsFor("A"));
        }
    }
}
