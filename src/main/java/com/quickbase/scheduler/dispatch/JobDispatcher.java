package com.quickbase.scheduler.dispatch;

import com.quickbase.scheduler.concurrency.ConcurrencyLimiter;
import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.BlockReason;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.domain.JobStatus;
import com.quickbase.scheduler.store.JobStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The single thread that decides what runs. Each pass drains a batch and judges
 * every job in it independently: one job blocked by a saturated target must not
 * stop the jobs behind it, which a strict poll-one-at-a-time loop cannot avoid.
 */
@Component
public class JobDispatcher {

    private static final Logger log = LoggerFactory.getLogger(JobDispatcher.class);

    private final JobQueue queue;
    private final JobStore store;
    private final ConcurrencyLimiter limiter;
    private final JobRunner runner;
    private final int drainBatchSize;
    private final long parkTimeoutMillis;

    private final Thread thread = new Thread(this::loop, "job-dispatcher");
    private volatile boolean running = true;

    public JobDispatcher(JobQueue queue, JobStore store, ConcurrencyLimiter limiter,
                         JobRunner runner, SchedulerProperties properties) {
        this.queue = queue;
        this.store = store;
        this.limiter = limiter;
        this.runner = runner;
        this.drainBatchSize = properties.dispatcher().drainBatchSize();
        this.parkTimeoutMillis = properties.dispatcher().parkTimeoutMillis();
    }

    @PostConstruct
    void start() {
        thread.setDaemon(true);
        thread.start();
    }

    @PreDestroy
    void stop() {
        running = false;
        thread.interrupt();
    }

    private void loop() {
        while (running) {
            try {
                if (dispatchPass() == 0) {
                    queue.awaitWork(parkTimeoutMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // Never let one bad job kill the thread: if this loop dies nothing
                // is ever dispatched again and the failure is silent.
                log.error("dispatch pass failed", e);
            }
        }
    }

    private int dispatchPass() {
        List<String> batch = queue.drain(drainBatchSize);
        if (batch.isEmpty()) {
            return 0;
        }

        List<String> requeue = new ArrayList<>();
        int dispatched = 0;

        for (String jobId : batch) {
            Job job = store.find(jobId).orElse(null);
            if (job == null || job.status() != JobStatus.PENDING) {
                continue;
            }
            if (!job.isEligibleAt(Instant.now())) {
                requeue.add(jobId);
                continue;
            }

            ConcurrencyLimiter.Acquisition acquisition = limiter.tryAcquire(job);
            if (!acquisition.acquired()) {
                markBlocked(jobId, acquisition.reason());
                requeue.add(jobId);
                continue;
            }

            Optional<JobStore.Update> started =
                    store.update(jobId, current -> current.running(Instant.now()));
            if (started.isEmpty()) {
                acquisition.slot().close();
                continue;
            }

            runner.submit(started.get().job(), acquisition.slot());
            dispatched++;
        }

        requeue.forEach(queue::enqueue);
        // A pass that dispatched nothing did no work.
        if (dispatched > 0) {
            log.debug("dispatch pass: drained {}, dispatched {}, requeued {}",
                    batch.size(), dispatched, requeue.size());
        } else if (log.isTraceEnabled()) {
            log.trace("dispatch pass: drained {}, dispatched 0, requeued {}",
                    batch.size(), requeue.size());
        }
        return dispatched;
    }

    // Guarded on PENDING because Job.blocked() rewrites the status, and a job that
    // moved on since we read it must not be dragged back into the queue's view.
    private void markBlocked(String jobId, BlockReason reason) {
        store.update(jobId, current -> current.status() == JobStatus.PENDING
                ? current.blocked(reason, Instant.now())
                : current);
    }
}
