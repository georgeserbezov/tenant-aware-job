package com.quickbase.scheduler.sse;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CapacityView(
        int globalMax,
        int globalAvailable,
        int perTenantMax,
        Integer tenantAvailable,
        int perTargetMax) {}
