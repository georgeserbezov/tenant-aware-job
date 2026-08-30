package com.quickbase.scheduler.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record CreateJobRequest(
        @NotBlank @Size(max = 64) String tenantId,
        @NotBlank @Size(max = 64) String targetId,
        @NotBlank @Size(max = 128) String idempotencyKey,
        @NotNull @Size(max = 8192) String payload) {}
