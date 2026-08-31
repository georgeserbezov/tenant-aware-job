package com.quickbase.scheduler.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.domain.JobStatus;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** A permanently failing downstream must exhaust the attempt budget and stop. */
class RetryBoundsTest {

    private SchedulerHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void aFailingJobStopsAfterMaxAttempts() throws Exception {
        harness = SchedulerHarness.failing(3);

        List<String> ids = harness.submit("tenant-1", "target-a", 1);
        harness.awaitCompletion(1);

        Job job = harness.job(ids.get(0));
        assertThat(job.status()).isEqualTo(JobStatus.FAILED);
        assertThat(job.attempt()).isEqualTo(3);
        assertThat(job.lastError()).contains("forced failure");
    }

    @Test
    void permitsAreReturnedAfterEveryFailedAttempt() throws Exception {
        harness = SchedulerHarness.failing(3);

        List<String> ids = harness.submit("tenant-1", "target-a", 4);
        harness.awaitCompletion(ids.size());

        // 12 failed attempts released 12 slots. A double release or a leak here
        // would leave the counts wrong rather than merely slow.
        assertThat(harness.limiter.availableGlobal()).isEqualTo(4);
        assertThat(harness.limiter.availableForTenant("tenant-1")).isEqualTo(4);
        assertThat(harness.limiter.availableForTarget("target-a")).isEqualTo(4);
    }
}
