package com.quickbase.scheduler.observability;

import com.quickbase.scheduler.domain.BlockReason;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.store.JobStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Logs every job state change. */
@Component
public class JobLogger {

    private static final Logger log = LoggerFactory.getLogger(JobLogger.class);

    private final JobStore store;

    public JobLogger(JobStore store) {
        this.store = store;
    }

    @PostConstruct
    void start() {
        store.addListener(this::onJobChanged);
    }

    private void onJobChanged(Job job) {
        switch (job.status()) {
            case PENDING -> logPending(job);
            case RUNNING -> log.info("job {} {} running, attempt {}/{}",
                    job.shortId(), route(job), job.attempt(), job.maxAttempts());
            case SUCCEEDED -> log.info("job {} {} succeeded on attempt {}",
                    job.shortId(), route(job), job.attempt());
            case FAILED -> log.warn("job {} {} failed after {} attempts: {}",
                    job.shortId(), route(job), job.attempt(), job.lastError());
        }
    }

    private void logPending(Job job) {
        if (job.blockReason() != BlockReason.NONE) {
            log.info("job {} {} waiting on {}", job.shortId(), route(job), job.blockReason());
        } else if (job.lastError() != null) {
            log.info("job {} {} will retry as attempt {}/{}, last error: {}",
                    job.shortId(), route(job), job.attempt() + 1, job.maxAttempts(), job.lastError());
        } else {
            log.info("job {} {} accepted", job.shortId(), route(job));
        }
    }

    private static String route(Job job) {
        return job.tenantId() + "/" + job.targetId();
    }
}
