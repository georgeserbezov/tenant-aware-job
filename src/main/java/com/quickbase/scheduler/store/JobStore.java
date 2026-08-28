package com.quickbase.scheduler.store;

import com.quickbase.scheduler.domain.Job;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import org.springframework.stereotype.Component;

@Component
public class JobStore {

    private static final Comparator<Job> BY_ARRIVAL =
            Comparator.comparing(Job::createdAt).thenComparing(Job::id);

    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> idempotencyIndex = new ConcurrentHashMap<>();

    public record Insert(Job job, boolean created) {}

    public record Update(Job job, boolean visiblyChanged) {}

    public Insert createOrGet(String tenantId, String targetId, String idempotencyKey,
                              String payload, int maxAttempts, Instant now) {
        AtomicBoolean created = new AtomicBoolean(false);

        // Creating the job and publishing it must be one atomic step. With a plain
        // putIfAbsent the loser gets an id back before the winner has written the
        // job, so an immediate GET on that id 404s.
        String id = idempotencyIndex.computeIfAbsent(scopedKey(tenantId, idempotencyKey), key -> {
            Job job = Job.pending(tenantId, targetId, idempotencyKey, payload, maxAttempts, now);
            jobs.put(job.id(), job);
            created.set(true);
            return job.id();
        });

        return new Insert(jobs.get(id), created.get());
    }

    public Optional<Update> update(String id, UnaryOperator<Job> transition) {
        AtomicBoolean visible = new AtomicBoolean(false);

        Job updated = jobs.compute(id, (key, current) -> {
            if (current == null) {
                return null;
            }
            Job next = transition.apply(current);
            visible.set(visiblyChanged(current, next));
            return next;
        });

        return updated == null
                ? Optional.empty()
                : Optional.of(new Update(updated, visible.get()));
    }

    public Optional<Job> find(String id) {
        return Optional.ofNullable(jobs.get(id));
    }

    public List<Job> all() {
        return jobs.values().stream().sorted(BY_ARRIVAL).toList();
    }

    public List<Job> byTenant(String tenantId) {
        return jobs.values().stream()
                .filter(job -> job.tenantId().equals(tenantId))
                .sorted(BY_ARRIVAL)
                .toList();
    }

    // Scoped per tenant so two tenants can both send "retry-1" without colliding.
    private static String scopedKey(String tenantId, String idempotencyKey) {
        return tenantId + ":" + idempotencyKey;
    }

    // Only the fields the stream actually renders. The dispatcher rewrites blocked
    // jobs every pass; broadcasting those would be pure noise.
    private static boolean visiblyChanged(Job before, Job after) {
        return before.status() != after.status()
                || before.blockReason() != after.blockReason()
                || before.attempt() != after.attempt();
    }
}
