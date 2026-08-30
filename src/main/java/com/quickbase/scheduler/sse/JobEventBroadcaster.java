package com.quickbase.scheduler.sse;

import com.quickbase.scheduler.concurrency.ConcurrencyLimiter;
import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.store.JobStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Sends job state changes out to connected browsers.
 */
@Component
public class JobEventBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(JobEventBroadcaster.class);

    private final JobStore store;
    private final ConcurrencyLimiter limiter;
    private final SchedulerProperties properties;

    private final CopyOnWriteArrayList<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService broadcast = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "sse-broadcast");
        thread.setDaemon(true);
        return thread;
    });

    public JobEventBroadcaster(JobStore store, ConcurrencyLimiter limiter, SchedulerProperties properties) {
        this.store = store;
        this.limiter = limiter;
        this.properties = properties;
    }

    @PostConstruct
    void start() {
        store.addListener(this::onJobChanged);
        long interval = properties.sse().heartbeatIntervalMillis();
        broadcast.scheduleAtFixedRate(this::heartbeat, interval, interval, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        broadcast.shutdownNow();
        subscribers.forEach(subscriber -> subscriber.emitter.complete());
        subscribers.clear();
    }

    public SseEmitter subscribe(String tenantId, boolean allTenants) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        Subscriber subscriber = new Subscriber(emitter, tenantId, allTenants);

        // All four teardown paths must remove the subscriber.
        emitter.onCompletion(() -> subscribers.remove(subscriber));
        emitter.onTimeout(() -> {
            subscribers.remove(subscriber);
            emitter.complete();
        });
        emitter.onError(e -> subscribers.remove(subscriber));

        broadcast.execute(() -> {
            subscribers.add(subscriber);
            List<Job> visible = subscriber.allTenants ? store.all() : store.byTenant(subscriber.tenantId);
            if (send(subscriber, "snapshot", visible)) {
                sendCapacityIfChanged(subscriber);
            }
        });
        return emitter;
    }

    // Called from worker and dispatcher threads: hand off and return immediately.
    private void onJobChanged(Job job) {
        broadcast.execute(() -> {
            for (Subscriber subscriber : subscribers) {
                if (subscriber.sees(job) && send(subscriber, "job-update", job)) {
                    sendCapacityIfChanged(subscriber);
                }
            }
        });
    }

    private void heartbeat() {
        for (Subscriber subscriber : subscribers) {
            try {
                subscriber.emitter.send(SseEmitter.event().comment("ping"));
            } catch (IOException | IllegalStateException e) {
                drop(subscriber);
            }
        }
    }

    private void sendCapacityIfChanged(Subscriber subscriber) {
        CapacityView current = capacityFor(subscriber);
        if (!Objects.equals(current, subscriber.lastCapacity)) {
            if (send(subscriber, "capacity", current)) {
                subscriber.lastCapacity = current;
            }
        }
    }

    private CapacityView capacityFor(Subscriber subscriber) {
        SchedulerProperties.Concurrency limits = properties.concurrency();
        return new CapacityView(
                limits.globalMax(),
                limiter.availableGlobal(),
                limits.perTenantMax(),
                subscriber.allTenants ? null : limiter.availableForTenant(subscriber.tenantId),
                limits.perTargetMax());
    }

    private boolean send(Subscriber subscriber, String event, Object payload) {
        try {
            subscriber.emitter.send(SseEmitter.event().name(event).data(payload));
            return true;
        } catch (IOException | IllegalStateException e) {
            // A client that vanished mid-send never fires a callback, so this is
            // the only path that catches it.
            drop(subscriber);
            return false;
        }
    }

    private void drop(Subscriber subscriber) {
        if (subscribers.remove(subscriber)) {
            try {
                subscriber.emitter.complete();
            } catch (RuntimeException ignored) {
                log.debug("emitter already closed");
            }
        }
    }

    public int subscriberCount() {
        return subscribers.size();
    }

    private static final class Subscriber {
        private final SseEmitter emitter;
        private final String tenantId;
        private final boolean allTenants;

        // Only ever touched on the broadcast thread, so it needs no synchronisation.
        private CapacityView lastCapacity;

        private Subscriber(SseEmitter emitter, String tenantId, boolean allTenants) {
            this.emitter = emitter;
            this.tenantId = tenantId;
            this.allTenants = allTenants;
        }

        private boolean sees(Job job) {
            return allTenants || job.tenantId().equals(tenantId);
        }
    }
}
