package com.quickbase.scheduler.concurrency;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/** The three permits one running job holds. Closing returns all three, once. */
public final class ConcurrencySlot implements AutoCloseable {

    private final Semaphore global;
    private final Semaphore tenant;
    private final Semaphore target;
    private final AtomicBoolean released = new AtomicBoolean(false);

    ConcurrencySlot(Semaphore global, Semaphore tenant, Semaphore target) {
        this.global = global;
        this.tenant = tenant;
        this.target = target;
    }

    @Override
    public void close() {
        // compareAndSet rather than a read-then-write: two threads can both read
        // false and both release, and Semaphore.release() has no ownership check,
        // so each extra call permanently widens the cap.
        if (!released.compareAndSet(false, true)) {
            return;
        }
        target.release();
        tenant.release();
        global.release();
    }
}
