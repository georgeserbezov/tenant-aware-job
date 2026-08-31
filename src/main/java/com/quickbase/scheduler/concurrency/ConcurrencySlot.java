package com.quickbase.scheduler.concurrency;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The three permits one running job holds. Closing returns all three, once. */
public final class ConcurrencySlot implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencySlot.class);

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
            log.warn("concurrency slot closed twice, permits were already returned");
            return;
        }
        target.release();
        tenant.release();
        global.release();
    }
}
