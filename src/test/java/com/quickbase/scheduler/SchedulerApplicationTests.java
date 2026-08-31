package com.quickbase.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.quickbase.scheduler.config.SchedulerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SchedulerApplicationTests {

    @Autowired
    private SchedulerProperties properties;

    @Test
    void contextLoadsAndEveryConfigBlockIsBound() {
        assertThat(properties.concurrency().globalMax()).isPositive();
        assertThat(properties.retry().maxAttempts()).isPositive();
        assertThat(properties.downstream().maxLatencyMillis()).isPositive();
        assertThat(properties.dispatcher().drainBatchSize()).isPositive();
        assertThat(properties.sse().heartbeatIntervalMillis()).isPositive();
    }
}
