package com.quickbase.scheduler.concurrency;

import com.quickbase.scheduler.config.SchedulerProperties;
import com.quickbase.scheduler.domain.BlockReason;
import com.quickbase.scheduler.domain.Job;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.springframework.stereotype.Component;

@Component
public class ConcurrencyLimiter {

    private final Semaphore global;
    private final int perTenantMax;
    private final int perTargetMax;

    // Never pruned. Eviction is unsafe while permits are outstanding, so this is
    // an accepted leak rather than a missed one.
    private final ConcurrentHashMap<String, Semaphore> tenantSemaphores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Semaphore> targetSemaphores = new ConcurrentHashMap<>();

    public ConcurrencyLimiter(SchedulerProperties properties) {
        SchedulerProperties.Concurrency limits = properties.concurrency();
        this.global = new Semaphore(limits.globalMax());
        this.perTenantMax = limits.perTenantMax();
        this.perTargetMax = limits.perTargetMax();
    }

    public record Acquisition(ConcurrencySlot slot, BlockReason reason) {

        public boolean acquired() {
            return slot != null;
        }

        static Acquisition granted(ConcurrencySlot slot) {
            return new Acquisition(slot, BlockReason.NONE);
        }

        static Acquisition blocked(BlockReason reason) {
            return new Acquisition(null, reason);
        }
    }

    /**
     * All three permits or none. Every acquisition is non-blocking, so no thread
     * ever holds one permit while waiting for another, which removes hold-and-wait
     * and makes deadlock structurally impossible rather than merely unlikely.
     */
    public Acquisition tryAcquire(Job job) {
        if (!global.tryAcquire()) {
            return Acquisition.blocked(BlockReason.GLOBAL_LIMIT);
        }

        Semaphore tenant = tenantSemaphores.computeIfAbsent(
                job.tenantId(), id -> new Semaphore(perTenantMax));
        if (!tenant.tryAcquire()) {
            global.release();
            return Acquisition.blocked(BlockReason.TENANT_LIMIT);
        }

        Semaphore target = targetSemaphores.computeIfAbsent(
                job.targetId(), id -> new Semaphore(perTargetMax));
        if (!target.tryAcquire()) {
            tenant.release();
            global.release();
            return Acquisition.blocked(BlockReason.TARGET_LIMIT);
        }

        return Acquisition.granted(new ConcurrencySlot(global, tenant, target));
    }

    public int availableGlobal() {
        return global.availablePermits();
    }

    public int availableForTenant(String tenantId) {
        Semaphore semaphore = tenantSemaphores.get(tenantId);
        return semaphore == null ? perTenantMax : semaphore.availablePermits();
    }

    public int availableForTarget(String targetId) {
        Semaphore semaphore = targetSemaphores.get(targetId);
        return semaphore == null ? perTargetMax : semaphore.availablePermits();
    }
}
