package com.quickbase.scheduler.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.dispatch.JobQueue;
import com.quickbase.scheduler.service.JobService;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** The duplicate idempotency key race: many callers, one job, one queue entry. */
class IdempotencyRaceTest {

    private static final int THREADS = 32;

    private final JobStore store = new JobStore();
    private final JobQueue queue = new JobQueue();
    private final JobService service = new JobService(store, queue, properties());

    @Test
    void concurrentDuplicatesProduceExactlyOneJob() throws Exception {
        List<JobStore.Insert> inserts =
                race(THREADS, i -> service.submit("tenant-1", "target-a", "order-42", "payload"));

        assertThat(inserts).filteredOn(JobStore.Insert::created).hasSize(1);
        assertThat(inserts).extracting(insert -> insert.job().id()).containsOnly(firstId(inserts));
        assertThat(store.byTenant("tenant-1")).hasSize(1);

        // Only the creator enqueues, or the job would run once per duplicate request.
        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    void everyCallerGetsAJobItCanImmediatelyReadBack() throws Exception {
        List<JobStore.Insert> inserts =
                race(THREADS, i -> service.submit("tenant-1", "target-a", "order-42", "payload"));

        // The losers of the race must never see an id before the job behind it is
        // written - the reason creation happens inside computeIfAbsent.
        assertThat(inserts).allSatisfy(insert ->
                assertThat(store.find(insert.job().id())).isPresent());
    }

    @Test
    void theSameKeyFromTwoTenantsDoesNotCollide() throws Exception {
        List<JobStore.Insert> inserts = race(THREADS, i ->
                service.submit(i % 2 == 0 ? "tenant-1" : "tenant-2", "target-a", "order-42", "payload"));

        assertThat(inserts).filteredOn(JobStore.Insert::created).hasSize(2);
        assertThat(inserts).extracting(insert -> insert.job().id()).doesNotContainNull();
        assertThat(distinctIds(inserts)).hasSize(2);
        assertThat(store.byTenant("tenant-1")).hasSize(1);
        assertThat(store.byTenant("tenant-2")).hasSize(1);
        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    void replayingAKeyReturnsTheOriginalAndIgnoresTheNewValues() {
        JobStore.Insert first = service.submit("tenant-1", "target-a", "order-42", "first");
        JobStore.Insert replay = service.submit("tenant-1", "target-b", "order-42", "second");

        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(first.job().id());
        assertThat(replay.job().targetId()).isEqualTo("target-a");
        assertThat(replay.job().payload()).isEqualTo("first");
    }

    private static String firstId(List<JobStore.Insert> inserts) {
        return inserts.get(0).job().id();
    }

    private static Set<String> distinctIds(List<JobStore.Insert> inserts) {
        return inserts.stream().map(insert -> insert.job().id()).collect(Collectors.toSet());
    }

    /** Parks every thread on the same latch so the calls genuinely overlap. */
    private static <T> List<T> race(int threads, IntFunction<T> call) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                int index = i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return call.apply(index);
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();

            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get(10, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    private static SchedulerProperties properties() {
        return new SchedulerProperties(
                new SchedulerProperties.Concurrency(8, 3, 1),
                new SchedulerProperties.Retry(3, 10),
                new SchedulerProperties.Downstream(0, 0, 0, 1),
                new SchedulerProperties.Dispatcher(64, 10),
                new SchedulerProperties.Sse(15_000));
    }
}
