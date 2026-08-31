package com.quickbase.scheduler.dispatch;

import com.quickbase.scheduler.concurrency.ConcurrencySlot;
import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.domain.JobStatus;
import com.quickbase.scheduler.downstream.FakeDownstreamService;
import com.quickbase.scheduler.store.JobStore;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Executes one attempt of a job on a worker thread and applies the outcome. */
@Component
public class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private final JobStore store;
    private final JobQueue queue;
    private final FakeDownstreamService downstream;
    private final long backoffBaseMillis;
    private final ExecutorService workers;

    public JobRunner(JobStore store, JobQueue queue, FakeDownstreamService downstream,
                     SchedulerProperties properties) {
        this.store = store;
        this.queue = queue;
        this.downstream = downstream;
        this.backoffBaseMillis = properties.retry().backoffBaseMillis();

        // Sized to the global cap so the pool can never be the binding constraint:
        // the dispatcher only submits while holding a global permit, so the
        // semaphore stays the one place concurrency is decided.
        AtomicInteger counter = new AtomicInteger();
        this.workers = Executors.newFixedThreadPool(
                properties.concurrency().globalMax(),
                runnable -> {
                    Thread thread = new Thread(runnable, "job-worker-" + counter.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
    }

    public void submit(Job job, ConcurrencySlot slot) {
        workers.execute(() -> run(job, slot));
    }

    private void run(Job job, ConcurrencySlot slot) {
        String error = null;
        long startedAt = System.nanoTime();
        try (slot) {
            downstream.execute(job);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = "interrupted during shutdown";
        } catch (RuntimeException e) {
            error = e.getMessage();
        }

        if (log.isDebugEnabled()) {
            log.debug("job {} attempt {} took {}ms, outcome {}", job.shortId(), job.attempt(),
                    (System.nanoTime() - startedAt) / 1_000_000, error == null ? "ok" : error);
        }

        // The slot is already closed by this point - try-with-resources releases
        // before anything below runs - so the dispatcher we wake at the end sees
        // the freed permits rather than racing us for them.
        try {
            if (error == null) {
                store.update(job.id(), current -> current.succeeded(Instant.now()));
            } else {
                finishAttempt(job.id(), error);
            }
        } catch (RuntimeException e) {
            log.error("failed to record outcome for job {}", job.id(), e);
        } finally {
            queue.signal();
        }
    }

    private void finishAttempt(String jobId, String error) {
        store.update(jobId, current -> {
            Instant now = Instant.now();
            if (!current.hasAttemptsLeft()) {
                return current.failed(error, now);
            }
            return current.retryAt(now.plusMillis(backoffMillis(current.attempt())), error, now);
        }).ifPresent(update -> {
            if (update.job().status() == JobStatus.PENDING) {
                queue.enqueue(jobId);
            }
        });
    }

    // Exponential on the attempt just consumed: 250ms, 500ms, 1s...
    private long backoffMillis(int attempt) {
        return backoffBaseMillis * (1L << Math.min(attempt - 1, 10));
    }

    @PreDestroy
    void shutdown() {
        workers.shutdownNow();
    }
}
