package edu.cs6510.events;

import java.util.TreeMap;
//跟踪分区中已完成的消息，避免越过尚未完成的消息提交 offset
//只计算和记录进度，实际提交 offset、暂停消费和重新定位消费位置的是 KafkaBus
/** Owned by one consumer thread; completed work may only advance a contiguous offset prefix. */
final class PartitionProgress {
    private final TreeMap<Long, Boolean> outstanding = new TreeMap<>();
    private int inFlight;
    private long completedOffset = -1;
    private long failedOffset = -1;
    private long retryAfterNanos;
    // 登记已分派的消息
    void dispatched(long offset) {
        if (outstanding.putIfAbsent(offset, false) != null)
            throw new IllegalStateException("Offset already dispatched: " + offset);
        inFlight++;
    }
    // 登记执行成功或失败
    void finished(long offset, boolean successful, long nowNanos) {
        if (!outstanding.containsKey(offset)) return;
        inFlight--;
        if (successful) outstanding.put(offset, true);
        else {
            failedOffset = failedOffset < 0 ? offset : Math.min(failedOffset, offset);
            retryAfterNanos = nowNanos + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(500);
        }
        while (!outstanding.isEmpty() && outstanding.firstEntry().getValue()) {
            // 返回当前可安全提交的位置
            completedOffset = outstanding.pollFirstEntry().getKey() + 1;
        }
    }

    int inFlight() { return inFlight; }
    long completedOffset() { return completedOffset; }
    boolean failed() { return failedOffset >= 0; }
    // 判断失败消息是否可以重试
    boolean retryReady(long nowNanos) { return failed() && inFlight == 0 && nowNanos >= retryAfterNanos; }
    // 返回需要重新消费的位置
    long restart() {
        if (!failed() || inFlight != 0) throw new IllegalStateException("Cannot replay unfinished work");
        long offset = failedOffset;
        outstanding.clear();
        failedOffset = -1;
        return offset;
    }
}
