namespace AnisShop.Kafka.Sessions.Tests
{
    // The single rule that keeps parallel session handling safe on an in-order partition cursor:
    // the cursor may never pass a message we have not finished with.
    public class OffsetWatermarkTests
    {
        [Fact]
        public void TryAdvance_NothingPending_MovesPastTheWholeBatch()
        {
            // Arrange: offsets 0-9 all handled
            // Act
            var advanced = OffsetWatermark.TryAdvance(
                highestBatchOffset: 9,
                lowestPendingOffset: null,
                storedPosition: OffsetWatermark.Unset,
                out var nextPosition);

            // Assert: next read starts after the batch
            Assert.True(advanced);
            Assert.Equal(10, nextPosition);
        }

        [Fact]
        public void TryAdvance_SomethingPending_StopsBelowTheOldestPendingOffset()
        {
            // Arrange: offsets 0-9 handled except offset 4, whose session's handler threw. Offsets
            // 5-9 belong to other sessions and are done, but the cursor cannot reflect that —
            // moving past 4 would lose it on restart.

            // Act
            var advanced = OffsetWatermark.TryAdvance(
                highestBatchOffset: 9,
                lowestPendingOffset: 4,
                storedPosition: OffsetWatermark.Unset,
                out var nextPosition);

            // Assert: 5-9 will simply be re-read and re-handed to an idempotent handler
            Assert.True(advanced);
            Assert.Equal(4, nextPosition);
        }

        [Theory]
        [InlineData(9L, null, 10L)]
        [InlineData(9L, 7L, 10L)]
        [InlineData(20L, 10L, 10L)]
        public void TryAdvance_WouldNotMoveForward_RefusesToRewind(
            long highestBatchOffset,
            long? lowestPendingOffset,
            long storedPosition)
        {
            // Arrange: a stored position already at or ahead of the computed one — which is what a
            // redelivered batch looks like after a pause/resume or a rebalance.

            // Act
            var advanced = OffsetWatermark.TryAdvance(
                highestBatchOffset, lowestPendingOffset, storedPosition, out _);

            // Assert: rewinding would replay work that is already done
            Assert.False(advanced);
        }

        [Fact]
        public void TryAdvance_FirstEverBatch_StartsFromUnset()
        {
            // Arrange + Act: a freshly assigned partition has stored nothing yet
            var advanced = OffsetWatermark.TryAdvance(
                highestBatchOffset: 0,
                lowestPendingOffset: null,
                storedPosition: OffsetWatermark.Unset,
                out var nextPosition);

            // Assert
            Assert.True(advanced);
            Assert.Equal(1, nextPosition);
        }
    }
}
