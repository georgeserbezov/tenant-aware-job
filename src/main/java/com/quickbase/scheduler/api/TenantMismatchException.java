package com.quickbase.scheduler.api;

public class TenantMismatchException extends RuntimeException {

    public TenantMismatchException(String header, String body) {
        super("X-Tenant-Id header '" + header + "' does not match body tenantId '" + body + "'");
    }
}
