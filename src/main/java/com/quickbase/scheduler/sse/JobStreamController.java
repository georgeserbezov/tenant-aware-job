package com.quickbase.scheduler.sse;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class JobStreamController {

    private final JobEventBroadcaster broadcaster;

    public JobStreamController(JobEventBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    /**
     * Takes the tenant from a query parameter as well as the header, because the
     * browser's EventSource cannot set headers.
     */
    @GetMapping("/jobs/stream")
    public SseEmitter stream(
            @RequestHeader(value = "X-Tenant-Id", required = false) String header,
            @RequestParam(required = false) String tenantId,
            @RequestParam(defaultValue = "false") boolean allTenants) {

        String tenant = header != null ? header : tenantId;
        if (tenant == null || tenant.isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Provide the tenant via the X-Tenant-Id header or a tenantId parameter");
        }
        return broadcaster.subscribe(tenant, allTenants);
    }
}
