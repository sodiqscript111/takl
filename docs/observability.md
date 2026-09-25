# Observability

Takl exposes Prometheus metrics at:

```text
GET /metrics
```

The endpoint includes HTTP status counts, placement request count, placement error count, placement latency, sync round count, sync round errors, sync rows/events applied, checksum reconciliation count, active runners, worker capacity, and available workers.

Runtime profiling is opt-in and requires an admin token:

```yaml
network:
  admin_token: "change-me"
  enable_pprof: true
```

Equivalent CLI flags:

```bash
takld --admin-token change-me --enable-pprof
```

Collect a placement CPU and heap profile on Windows:

```powershell
.\scripts\profile-placement.ps1 -AdminToken change-me -Seconds 30 -Concurrency 8
```

Collect a placement CPU and heap profile on Linux/macOS:

```bash
ADMIN_TOKEN=change-me SECONDS=30 CONCURRENCY=8 ./scripts/profile-placement.sh
```

Open the pprof UI and use the flame graph view:

```bash
go tool pprof -http=:0 profiles/<run>/cpu.pprof
```
