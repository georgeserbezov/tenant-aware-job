package com.quickbase.scheduler.downstream;

import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.Job;
import java.util.Random;
import org.springframework.stereotype.Component;

/**
 * Stands in for the slow, unreliable third party the scheduler protects. Blocking
 * on purpose: a worker thread holding its three permits for the whole call is
 * exactly the condition the caps exist to bound.
 */
@Component
public class FakeDownstreamService {

    private final Random random;
    private final long minLatencyMillis;
    private final long latencySpread;
    private final double failureRate;

    public FakeDownstreamService(SchedulerProperties properties) {
        SchedulerProperties.Downstream config = properties.downstream();
        this.minLatencyMillis = config.minLatencyMillis();
        this.latencySpread = Math.max(0, config.maxLatencyMillis() - config.minLatencyMillis());
        this.failureRate = config.failureRate();
        // Seed 0 means "give me a different run every time"; any other value fixes
        // the latency and failure sequence. Thread interleaving still varies, so
        // this makes demos comparable, not bit-for-bit reproducible.
        this.random = config.seed() == 0 ? new Random() : new Random(config.seed());
    }

    public void execute(Job job) throws InterruptedException {
        long latency;
        boolean fail;
        synchronized (random) {
            latency = minLatencyMillis + (latencySpread == 0 ? 0 : random.nextLong(latencySpread + 1));
            fail = random.nextDouble() < failureRate;
        }

        Thread.sleep(latency);

        if (fail) {
            throw new DownstreamException(
                    "downstream call failed for target " + job.targetId() + " after " + latency + "ms");
        }
    }

    public static class DownstreamException extends RuntimeException {
        public DownstreamException(String message) {
            super(message);
        }
    }
}
