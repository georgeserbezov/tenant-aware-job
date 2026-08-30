package com.quickbase.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        Concurrency concurrency,
        Retry retry,
        Downstream downstream,
        Dispatcher dispatcher) {

    public record Concurrency(int globalMax, int perTenantMax, int perTargetMax) {}

    public record Retry(int maxAttempts, long backoffBaseMillis) {}

    public record Downstream(
            long minLatencyMillis, long maxLatencyMillis, double failureRate, long seed) {}

    public record Dispatcher(int drainBatchSize, long parkTimeoutMillis) {}
}
