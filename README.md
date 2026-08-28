# Tenant-Aware Job Scheduler

A job scheduler that enforces **three simultaneous concurrency caps** — global,
per-tenant, and per-target — with idempotent enqueue, bounded retries, and a
live SSE status stream backed by a small React UI.

Spring Boot 3.5 on Java 21, in-memory by design.

## Run

```bash
./run.sh
```

Builds the React UI into the app's static resources and starts everything on
<http://localhost:8080> as a single process.

> **Status: work in progress.** Full architecture notes, concurrency design
> rationale, tradeoffs, and AI-usage disclosure land here as the build completes.
