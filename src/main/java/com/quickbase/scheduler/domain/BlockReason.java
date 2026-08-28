package com.quickbase.scheduler.domain;

/** Why a pending job did not start on the last dispatch attempt. */
public enum BlockReason {
    NONE,
    GLOBAL_LIMIT,
    TENANT_LIMIT,
    TARGET_LIMIT
}
