package com.quickbase.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Entry point for the tenant-aware job scheduler.
 *
 * <p>The application is intentionally a single process with no external
 * infrastructure: the queue, the job store and the concurrency permits all live
 * in memory. That is a deliberate scope decision for this exercise, not an
 * oversight - see the README for what breaks at more than one instance and what
 * would replace each piece in a real deployment.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class SchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }
}
