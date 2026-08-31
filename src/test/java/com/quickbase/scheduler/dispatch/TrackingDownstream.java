package com.quickbase.scheduler.dispatch;

import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.downstream.FakeDownstreamService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Replaces the random downstream with a fixed hold, and records how many calls
 * were in flight at once globally, per tenant and per target. Those high-water
 * marks are what the cap tests assert on.
 */
final class TrackingDownstream extends FakeDownstreamService {

    private final long holdMillis;
    private final boolean alwaysFail;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger globalPeak = new AtomicInteger();
    private final ConcurrentHashMap<String, AtomicInteger> tenantInFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> tenantPeak = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> targetInFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> targetPeak = new ConcurrentHashMap<>();

    TrackingDownstream(SchedulerProperties properties, long holdMillis, boolean alwaysFail) {
        super(properties);
        this.holdMillis = holdMillis;
        this.alwaysFail = alwaysFail;
    }

    @Override
    public void execute(Job job) throws InterruptedException {
        enter(job);
        try {
            Thread.sleep(holdMillis);
        } finally {
            leave(job);
        }
        if (alwaysFail) {
            throw new DownstreamException("forced failure for target " + job.targetId());
        }
    }

    // Counted inside the call, which is a strict subset of the window the permits
    // are held for. It can only ever under-report, never invent concurrency.
    private void enter(Job job) {
        raise(globalPeak, inFlight.incrementAndGet());
        raise(counter(tenantPeak, job.tenantId()), counter(tenantInFlight, job.tenantId()).incrementAndGet());
        raise(counter(targetPeak, job.targetId()), counter(targetInFlight, job.targetId()).incrementAndGet());
    }

    private void leave(Job job) {
        inFlight.decrementAndGet();
        counter(tenantInFlight, job.tenantId()).decrementAndGet();
        counter(targetInFlight, job.targetId()).decrementAndGet();
    }

    private static void raise(AtomicInteger peak, int observed) {
        peak.accumulateAndGet(observed, Math::max);
    }

    private static AtomicInteger counter(ConcurrentHashMap<String, AtomicInteger> counters, String key) {
        return counters.computeIfAbsent(key, unused -> new AtomicInteger());
    }

    int globalPeak() {
        return globalPeak.get();
    }

    int tenantPeak(String tenantId) {
        return counter(tenantPeak, tenantId).get();
    }

    int targetPeak(String targetId) {
        return counter(targetPeak, targetId).get();
    }

    int highestTenantPeak() {
        return highest(tenantPeak);
    }

    int highestTargetPeak() {
        return highest(targetPeak);
    }

    private static int highest(ConcurrentHashMap<String, AtomicInteger> peaks) {
        return peaks.values().stream().mapToInt(AtomicInteger::get).max().orElse(0);
    }
}
