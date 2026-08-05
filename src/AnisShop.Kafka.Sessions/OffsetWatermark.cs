namespace AnisShop.Kafka.Sessions;

// A Kafka partition has one cursor, but the sessions inside it are deliberately handled in
// parallel and therefore finish out of order. The cursor may only ever move to just below the
// oldest message we have *not* finished with — otherwise a restart would skip it.
public static class OffsetWatermark
{
    // Nothing has been stored for this partition yet. Offsets are non-negative, so any real
    // position beats it.
    public const long Unset = -1;

    // Kafka commits a *position*: the offset of the next message to read. Everything strictly
    // below the result has been handed to a handler that returned; everything from it upwards is
    // re-read after a restart or rebalance. That replay is why handlers must be idempotent —
    // the same at-least-once contract a session receiver has.
    public static bool TryAdvance(
        long highestBatchOffset,
        long? lowestPendingOffset,
        long storedPosition,
        out long nextPosition)
    {
        nextPosition = lowestPendingOffset ?? highestBatchOffset + 1;

        return nextPosition > storedPosition;
    }
}
