package com.quickbase.scheduler.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable: JobStore replaces whole instances inside ConcurrentHashMap.compute(),
 * so a status change and an attempt increment land as one atomic step, and the
 * instance handed to the SSE serializer cannot be mutated by a worker mid-write.
 */
public record Job(
        String id,
        String tenantId,
        String targetId,
        String idempotencyKey,
        String payload,
        JobStatus status,
        BlockReason blockReason,
        int attempt,
        int maxAttempts,
        Instant nextEligibleAt,
        String lastError,
        Instant createdAt,
        Instant updatedAt) {

    public Job {
        Objects.requireNonNull(id);
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(targetId);
        Objects.requireNonNull(status);
        Objects.requireNonNull(blockReason);
    }

    public static Job pending(String tenantId, String targetId, String idempotencyKey,
                              String payload, int maxAttempts, Instant now) {
        return new Job(
                UUID.randomUUID().toString(),
                tenantId,
                targetId,
                idempotencyKey,
                payload,
                JobStatus.PENDING,
                BlockReason.NONE,
                0,
                maxAttempts,
                now,
                null,
                now,
                now);
    }

    public Job blocked(BlockReason reason, Instant now) {
        return new Job(id, tenantId, targetId, idempotencyKey, payload,
                JobStatus.PENDING, reason, attempt, maxAttempts,
                nextEligibleAt, lastError, createdAt, now);
    }

    public Job running(Instant now) {
        return new Job(id, tenantId, targetId, idempotencyKey, payload,
                JobStatus.RUNNING, BlockReason.NONE, attempt + 1, maxAttempts,
                nextEligibleAt, lastError, createdAt, now);
    }

    public Job succeeded(Instant now) {
        return new Job(id, tenantId, targetId, idempotencyKey, payload,
                JobStatus.SUCCEEDED, BlockReason.NONE, attempt, maxAttempts,
                nextEligibleAt, null, createdAt, now);
    }

    public Job retryAt(Instant eligibleAt, String error, Instant now) {
        return new Job(id, tenantId, targetId, idempotencyKey, payload,
                JobStatus.PENDING, BlockReason.NONE, attempt, maxAttempts,
                eligibleAt, error, createdAt, now);
    }

    public Job failed(String error, Instant now) {
        return new Job(id, tenantId, targetId, idempotencyKey, payload,
                JobStatus.FAILED, BlockReason.NONE, attempt, maxAttempts,
                nextEligibleAt, error, createdAt, now);
    }

    public boolean hasAttemptsLeft() {
        return attempt < maxAttempts;
    }

    public boolean isEligibleAt(Instant now) {
        return nextEligibleAt == null || !now.isBefore(nextEligibleAt);
    }
}
