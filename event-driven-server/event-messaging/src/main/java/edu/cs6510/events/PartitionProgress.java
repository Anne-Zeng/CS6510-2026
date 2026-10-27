package edu.cs6510.events;

import java.util.TreeMap;

/** Owned by one consumer thread; completed work may only advance a contiguous offset prefix. */
final class PartitionProgress {
    private final TreeMap<Long, Boolean> outstanding = new TreeMap<>();
    private int inFlight;
    private long completedOffset = -1;
    private long failedOffset = -1;
    private long retryAfterNanos;

    void dispatched(long offset) {
        if (outstanding.putIfAbsent(offset, false) != null)
            throw new IllegalStateException("Offset already dispatched: " + offset);
        inFlight++;
    }

    void finished(long offset, boolean successful, long nowNanos) {
        if (!outstanding.containsKey(offset)) return;
        inFlight--;
        if (successful) outstanding.put(offset, true);
        else {
            failedOffset = failedOffset < 0 ? offset : Math.min(failedOffset, offset);
            retryAfterNanos = nowNanos + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(500);
        }
        while (!outstanding.isEmpty() && outstanding.firstEntry().getValue()) {
            completedOffset = outstanding.pollFirstEntry().getKey() + 1;
        }
    }

    int inFlight() { return inFlight; }
    long completedOffset() { return completedOffset; }
    boolean failed() { return failedOffset >= 0; }
    boolean retryReady(long nowNanos) { return failed() && inFlight == 0 && nowNanos >= retryAfterNanos; }

    long restart() {
        if (!failed() || inFlight != 0) throw new IllegalStateException("Cannot replay unfinished work");
        long offset = failedOffset;
        outstanding.clear();
        failedOffset = -1;
        return offset;
    }
}
