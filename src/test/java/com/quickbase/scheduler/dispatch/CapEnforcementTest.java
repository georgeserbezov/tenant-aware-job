package com.quickbase.scheduler.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.quickbase.scheduler.domain.BlockReason;
import com.quickbase.scheduler.domain.JobStatus;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Cap enforcement under burst load. Every case asserts the high-water mark
 * <em>equals</em> the cap: "never exceeded" alone would also pass on a run where
 * the cap was never reached, which proves nothing.
 */
class CapEnforcementTest {

    private static final long HOLD_MILLIS = 100;

    private SchedulerHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    @DisplayName("20 jobs at one target run two at a time")
    void perTargetCapBinds() throws Exception {
        harness = SchedulerHarness.with(8, 8, 2, HOLD_MILLIS);

        List<String> ids = harness.submit("tenant-1", "target-a", 20);
        harness.awaitCompletion(ids.size());

        assertThat(harness.downstream.targetPeak("target-a"))
                .as("concurrent calls to one target")
                .isEqualTo(2);
        assertThat(harness.blockReasons()).contains(BlockReason.TARGET_LIMIT);
        assertAllSucceeded(ids);
    }

    @Test
    @DisplayName("15 jobs across 15 targets still run three at a time for one tenant")
    void perTenantCapBinds() throws Exception {
        harness = SchedulerHarness.with(16, 3, 1, HOLD_MILLIS);

        List<String> ids = harness.submitAcrossTargets("tenant-1", 15);
        harness.awaitCompletion(ids.size());

        assertThat(harness.downstream.tenantPeak("tenant-1"))
                .as("concurrent calls for one tenant")
                .isEqualTo(3);
        assertThat(harness.blockReasons()).contains(BlockReason.TENANT_LIMIT);
        assertAllSucceeded(ids);
    }

    @Test
    @DisplayName("18 jobs across three tenants never exceed the global cap")
    void globalCapBinds() throws Exception {
        harness = SchedulerHarness.with(4, 10, 1, HOLD_MILLIS);

        List<String> ids = List.of("tenant-1", "tenant-2", "tenant-3").stream()
                .flatMap(tenant -> harness.submitAcrossTargets(tenant, 6).stream())
                .toList();
        harness.awaitCompletion(ids.size());

        assertThat(harness.downstream.globalPeak())
                .as("concurrent calls across the whole server")
                .isEqualTo(4);
        assertThat(harness.blockReasons()).contains(BlockReason.GLOBAL_LIMIT);
        assertAllSucceeded(ids);
    }

    @Test
    @DisplayName("all three caps hold at once under mixed load")
    void allThreeCapsHoldTogether() throws Exception {
        harness = SchedulerHarness.with(5, 2, 1, HOLD_MILLIS);

        List<String> ids = List.of("tenant-1", "tenant-2", "tenant-3", "tenant-4").stream()
                .flatMap(tenant -> harness.submitAcrossTargets(tenant, 5).stream())
                .toList();
        harness.awaitCompletion(ids.size());

        // Four tenants could contribute 8 concurrent calls between them, so the
        // global cap is the one that has to bite here.
        assertThat(harness.downstream.globalPeak()).isEqualTo(5);
        assertThat(harness.downstream.highestTenantPeak()).isEqualTo(2);
        assertThat(harness.downstream.highestTargetPeak()).isEqualTo(1);
        assertAllSucceeded(ids);
    }

    @Test
    @DisplayName("a tenant queued behind a busy target does not hold up another tenant")
    void blockedJobsDoNotBlockTheQueueBehindThem() throws Exception {
        harness = SchedulerHarness.with(8, 8, 1, HOLD_MILLIS);

        List<String> serialised = harness.submit("tenant-1", "shared-target", 15);
        List<String> free = harness.submitAcrossTargets("tenant-2", 3);
        harness.awaitCompletion(serialised.size() + free.size());

        assertThat(harness.downstream.targetPeak("shared-target")).isEqualTo(1);

        int lastSerialised = serialised.stream().mapToInt(harness::completionRank).max().orElseThrow();
        int lastFree = free.stream().mapToInt(harness::completionRank).max().orElseThrow();
        assertThat(lastFree)
                .as("tenant-2 should finish while tenant-1 is still working through its target")
                .isLessThan(lastSerialised);
    }

    private void assertAllSucceeded(List<String> ids) {
        assertThat(ids).allSatisfy(id ->
                assertThat(harness.job(id).status()).isEqualTo(JobStatus.SUCCEEDED));
    }
}
