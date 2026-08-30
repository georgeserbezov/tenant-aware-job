package com.quickbase.scheduler.sse;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class JobStreamController {

    private final JobEventBroadcaster broadcaster;

    public JobStreamController(JobEventBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping("/jobs/stream")
    public SseEmitter stream(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(defaultValue = "false") boolean allTenants) {
        return broadcaster.subscribe(tenantId, allTenants);
    }
}
