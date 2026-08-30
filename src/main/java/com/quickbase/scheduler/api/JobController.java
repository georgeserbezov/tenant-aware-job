package com.quickbase.scheduler.api;

import com.quickbase.scheduler.domain.Job;
import com.quickbase.scheduler.service.JobService;
import com.quickbase.scheduler.store.JobStore;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobService jobs;

    public JobController(JobService jobs) {
        this.jobs = jobs;
    }

    @PostMapping
    public ResponseEntity<Job> create(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @Valid @RequestBody CreateJobRequest request) {

        // The header is the identity; the body field only gets to agree with it.
        if (!tenantId.equals(request.tenantId())) {
            throw new TenantMismatchException(tenantId, request.tenantId());
        }

        JobStore.Insert insert = jobs.submit(
                tenantId, request.targetId(), request.idempotencyKey(), request.payload());

        // 201 for a job this call created, 200 for an idempotent replay
        return insert.created()
                ? ResponseEntity.created(URI.create("/jobs/" + insert.job().id())).body(insert.job())
                : ResponseEntity.ok(insert.job());
    }

    @GetMapping
    public List<Job> list(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(defaultValue = "false") boolean allTenants) {
        return jobs.list(tenantId, allTenants);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Job> get(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @PathVariable String id) {
        return jobs.find(id, tenantId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
