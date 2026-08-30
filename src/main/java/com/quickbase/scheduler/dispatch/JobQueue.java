package com.quickbase.scheduler.dispatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * Pending job ids waiting for a dispatch decision, plus the condition the single
 * dispatcher thread parks on. Holding ids rather than Job instances keeps the
 * JobStore the only source of truth for state.
 */
@Component
public class JobQueue {

    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition work = lock.newCondition();

    public void enqueue(String jobId) {
        queue.offer(jobId);
        signal();
    }

    /** Wakes the dispatcher without adding work — used when a worker frees a slot. */
    public void signal() {
        lock.lock();
        try {
            work.signal();
        } finally {
            lock.unlock();
        }
    }

    public List<String> drain(int max) {
        List<String> batch = new ArrayList<>(Math.min(max, queue.size()));
        for (int i = 0; i < max; i++) {
            String id = queue.poll();
            if (id == null) {
                break;
            }
            batch.add(id);
        }
        return batch;
    }

    /**
     * Parks unconditionally rather than only when the queue is empty: after a pass
     * where every job was blocked the queue is non-empty but retrying immediately
     * would just spin. The timeout bounds the cost of a signal that lands between
     * the dispatcher's last poll and this await.
     */
    public void awaitWork(long timeoutMillis) throws InterruptedException {
        lock.lock();
        try {
            work.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return queue.size();
    }
}
