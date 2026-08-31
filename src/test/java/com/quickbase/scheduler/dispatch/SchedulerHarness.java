package com.quickbase.scheduler.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.quickbase.scheduler.concurrency.ConcurrencyLimiter;
import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.BlockReason;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.service.JobService;
import com.quickbase.scheduler.store.JobStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A whole scheduler wired by hand: real store, queue, limiter, dispatcher and
 * runner, with only the downstream swapped. No Spring context, so a test starts
 * in milliseconds and each one gets its own set of permits.
 */
final class SchedulerHarness implements AutoCloseable {

    private static final long AWAIT_SECONDS = 30;

    final JobStore store;
    final ConcurrencyLimiter limiter;
    final TrackingDownstream downstream;

    private final JobQueue queue;
    private final JobRunner runner;
    private final JobDispatcher dispatcher;
    private final JobService service;

    private final Set<String> terminal = ConcurrentHashMap.newKeySet();
    private final Set<BlockReason> blockReasons = ConcurrentHashMap.newKeySet();
    private final List<String> completionOrder = new CopyOnWriteArrayList<>();
    private final Semaphore completions = new Semaphore(0);

    private SchedulerHarness(SchedulerProperties properties, long holdMillis, boolean alwaysFail) {
        this.store = new JobStore();
        this.queue = new JobQueue();
        this.limiter = new ConcurrencyLimiter(properties);
        this.downstream = new TrackingDownstream(properties, holdMillis, alwaysFail);
        this.runner = new JobRunner(store, queue, downstream, properties);
        this.dispatcher = new JobDispatcher(queue, store, limiter, runner, properties);
        this.service = new JobService(store, queue, properties);

        store.addListener(this::record);
        dispatcher.start();
    }

    static SchedulerHarness with(int globalMax, int perTenantMax, int perTargetMax, long holdMillis) {
        return new SchedulerHarness(properties(globalMax, perTenantMax, perTargetMax, 3), holdMillis, false);
    }

    static SchedulerHarness failing(int maxAttempts) {
        return new SchedulerHarness(properties(4, 4, 4, maxAttempts), 1, true);
    }

    private static SchedulerProperties properties(
            int globalMax, int perTenantMax, int perTargetMax, int maxAttempts) {
        return new SchedulerProperties(
                new SchedulerProperties.Concurrency(globalMax, perTenantMax, perTargetMax),
                new SchedulerProperties.Retry(maxAttempts, 10),
                new SchedulerProperties.Downstream(0, 0, 0, 1),
                new SchedulerProperties.Dispatcher(64, 10),
                new SchedulerProperties.Sse(15_000));
    }

    // The store publishes on the caller's thread, so this stays to bookkeeping.
    private void record(Job job) {
        if (job.blockReason() != BlockReason.NONE) {
            blockReasons.add(job.blockReason());
        }
        if (job.status().isTerminal() && terminal.add(job.id())) {
            completionOrder.add(job.id());
            completions.release();
        }
    }

    List<String> submit(String tenantId, String targetId, int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(submitOne(tenantId, targetId, i));
        }
        return ids;
    }

    /** One job per target, so the per-target cap cannot be the binding constraint. */
    List<String> submitAcrossTargets(String tenantId, int targets) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < targets; i++) {
            ids.add(submitOne(tenantId, tenantId + "-target-" + i, i));
        }
        return ids;
    }

    private String submitOne(String tenantId, String targetId, int index) {
        return service.submit(tenantId, targetId, targetId + "-" + index, "payload").job().id();
    }

    /**
     * Blocks on a permit per completion rather than polling or sleeping: the test
     * asserts on what happened, and the timeout is only a stuck-build guard.
     */
    void awaitCompletion(int count) throws InterruptedException {
        assertThat(completions.tryAcquire(count, AWAIT_SECONDS, TimeUnit.SECONDS))
                .as("%d jobs should reach a terminal state (%d did)", count, terminal.size())
                .isTrue();
    }

    Job job(String id) {
        return store.find(id).orElseThrow();
    }

    Set<BlockReason> blockReasons() {
        return blockReasons;
    }

    int completionRank(String jobId) {
        return completionOrder.indexOf(jobId);
    }

    @Override
    public void close() {
        dispatcher.stop();
        runner.shutdown();
    }
}
