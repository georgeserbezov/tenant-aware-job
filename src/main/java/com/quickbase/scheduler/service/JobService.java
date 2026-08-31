package com.quickbase.scheduler.service;

import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.dispatch.JobQueue;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.store.JobStore;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobStore store;
    private final JobQueue queue;
    private final int maxAttempts;

    public JobService(JobStore store, JobQueue queue, SchedulerProperties properties) {
        this.store = store;
        this.queue = queue;
        this.maxAttempts = properties.retry().maxAttempts();
    }

    public JobStore.Insert submit(String tenantId, String targetId, String idempotencyKey, String payload) {
        JobStore.Insert insert =
                store.createOrGet(tenantId, targetId, idempotencyKey, payload, maxAttempts, Instant.now());

        // Only the creator enqueues. A duplicate request must not put a second
        // copy of the same id in the queue.
        if (insert.created()) {
            queue.enqueue(insert.job().id());
        } else {
            log.info("idempotent replay of key {} for {}, returning job {}",
                    idempotencyKey, tenantId, insert.job().shortId());
        }
        return insert;
    }

    // Filtered for better recognition between jobs
    public Optional<Job> find(String id, String tenantId) {
        return store.find(id).filter(job -> job.tenantId().equals(tenantId));
    }

    public List<Job> list(String tenantId, boolean allTenants) {
        return allTenants ? store.all() : store.byTenant(tenantId);
    }
}
