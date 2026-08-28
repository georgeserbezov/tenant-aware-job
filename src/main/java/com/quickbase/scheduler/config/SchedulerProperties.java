package com.quickbase.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(Concurrency concurrency) {

    public record Concurrency(int globalMax, int perTenantMax, int perTargetMax) {}
}
