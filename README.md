# Grailkeeper

**Dynatrace Grail Query, Extract & Ingest UI** — a browser-based interface for querying, extracting and ingesting data from Dynatrace Grail, powered by the `dtctl` CLI running inside a Docker container.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Docker](https://img.shields.io/badge/docker-required-blue.svg)
![dtctl](https://img.shields.io/badge/dtctl-v0.16.0-cyan.svg)
![Version](https://img.shields.io/badge/version-2.1-green.svg)

---

## What's New — v2.1

> **Download:** [grailkeeper-v2.1.zip](https://github.com/fragelo/grailkeeper/releases/tag/v2.1)

### Smart Extraction Engine (complete rewrite)
- **Pre-flight density profiling** — runs a summarize query before extraction to measure records-per-hour per interval and avg bytes/record for your specific DQL and tenant
- **Dynamic caps per window** — `max_records` computed from real density, not static values
- **Smart slot merging** — sparse queries (e.g. 40K records/day) build 1 window instead of 48 — 48× fewer API calls
- **Auto-split on truncation** — if a window exceeds the cap, splits recursively; parent caps passed unchanged to avoid cascading degradation
- **Background jobs** — extraction runs server-side; browser can close and reconnect
- **Pipe deadlock fix** — dtctl output written to temp file instead of PIPE, eliminating silent hangs on large responses
- **Instant Pause / Stop / Resume** — kills dtctl process immediately; resume continues from last completed window
- **Parallel workers (1/2/3)** — optional parallel window execution; same DQL license cost as sequential
- **Scheduling** — optional run window (e.g. 22:00–06:00 Europe/Rome) to avoid business hours

### Extraction proven at scale
> **39,607 records · 56.9 MB · 1 window · 1 API call · 0 splits · 19 seconds**
> Filtered CDR query on a tenant with 4.3 billion scanned records/day

### New UI — grouped sidebar navigation
```
⚡ Query    ▾   DQL Query · History
⛏ Extract  ▾   DQL Extract · History · Files · Jobs
📥 Ingest   ▾   Ingest · History
🔑 Settings ▾   Query & Extract · Ingest
```

### Extraction History
- Auto-saved on every completed extraction
- Shows DQL, date range, records, duration, delta%
- **▶ Replay** — pre-fills Extract form with same settings, new timestamped filename

### Ingest History
- Auto-saved on every ingest run; **▶ Replay** re-fills the form

### Extract > Jobs panel
- All jobs visible: running, paused, stopped, done
- **▶ View** — reconnects live polling for running jobs
- **▶ Resume** — resumes paused/stopped/interrupted jobs
- **Summary** — floating overlay with full extraction report including DQL
- **Delete** — removes job metadata and output file

### Other improvements
- Filename format: `extraction_21h25m34s_08-04-2026.jsonl` — no collisions
- Quick date buttons: **1d · 7d · 30d · 90d · 180d · 365d** — 1d is exact 24h
- Live log scroll lock (🔒 Lock)
- Full DQL visible in job header (scrollable)
- ETA displayed from window 1 based on observed call time
- Atomic state file writes — no corruption on Docker kill

---

## Features

| Panel | Description |
|-------|-------------|
| ⚡ **Query** | DQL editor with all dtctl parameters — Ctrl+Enter to run, live console, write to disk, table/raw toggle, export, query history |
| ⛏ **Extract** | Smart extraction engine — pre-flight density profiling, dynamic caps, auto-split, background jobs, parallel workers, pause/stop/resume, scheduling, extraction history |
| 📥 **Ingest** | Log file ingest via Dynatrace Logs API v2 — Sequential, Historic, Scattered modes, file upload, 5 demo datasets, attribute injection, ingest history |
| 📁 **Files/Jobs** | Per-extraction file management with Summary overlay, First/Last preview, Download, Delete; job management with View/Resume/Delete |
| 🔑 **Configure** | Two separate token configs: Platform Token for Query/Extract, Access Token for Ingest |

---

## Architecture

```
Browser (localhost:8000)
    ↕ HTTP (polling every 2s)
FastAPI Backend (/app/backend/app.py)
    ↕ subprocess (stdout → temp file, no PIPE buffer)
dtctl v0.16.0 (Linux amd64, /usr/local/bin/dtctl)
    ↕ HTTPS
Dynatrace Grail API (*.apps.dynatrace.com)
```

Persistent volume at `/root/.config/dtctl/`:
- `exports/` — extracted JSONL files (survive rebuilds)
- `jobs/` — job state, log, summary files
- `ingest_config.json` — ingest token

---

## Requirements

- Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- `docker compose` v2+
- 16 GB+ RAM recommended
- Internet access on first build (downloads dtctl binary from GitHub)

---

## Quick Start

```bash
cd grailkeeper
docker compose build --no-cache && docker compose up
```

Open **http://localhost:8000** — go to **Settings → Query & Extract** to configure your Platform Token.

---

## Configuration

### Query & Extract — Platform Token (`dt0s16.xxx`)

Required scopes:
```
storage:logs:read, storage:events:read, storage:metrics:read,
storage:spans:read, storage:bizevents:read, storage:entities:read,
storage:buckets:read, storage:bucket-definitions:read,
automation:workflows:read, document:documents:read,
slo:slos:read, settings:schemas:read, settings:objects:read
```

### Ingest — Access Token (`dt0c01.xxx` or `dt0e.xxx`)

Required scope: `logs.ingest`

---

## Smart Extraction — How it works

```
1. Pre-flight summarize query
   → scans full date range once
   → returns records-per-hour per interval + avg_bytes/record

2. Schedule builder
   → merges sparse intervals into larger windows (fewer API calls)
   → dense intervals stay small to avoid cap truncation
   → cap = expected_records × 3 (safety factor)

3. Extraction loop
   → one dtctl call per window (sequential or parallel)
   → output written to temp file (no pipe buffer deadlock)
   → truncation detected → recursive halving (auto-split)
   → results appended to JSONL file line by line

4. Resume
   → reads completed window_done events from log file
   → skips completed windows, continues from interruption point
```

### Recommended DQL pattern

```dql
fetch logs, scanLimitGBytes:-1, bucket:{"your-bucket"}
| filter imsi == "123456789012345"
| fields timestamp, content, imsi
```

- Always add `bucket:{}` — reduces scan from TB to GB
- Use `filter` not `search` for indexed fields — early stop
- Avoid `| limit N` — defeats completeness guarantee

---

## API Endpoints (v2.1)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Connection status |
| POST | `/api/config` | Save Platform Token config |
| POST | `/api/command/stream` | SSE — run dtctl query |
| POST | `/api/extract/start` | Start extraction job |
| GET | `/api/extract/job/{id}` | Poll job state |
| GET | `/api/extract/job/{id}/log` | Poll job log events |
| GET | `/api/extract/job/{id}/summary` | Extraction summary |
| POST | `/api/extract/job/{id}/pause` | Pause job |
| POST | `/api/extract/job/{id}/stop` | Stop job |
| POST | `/api/extract/job/{id}/resume` | Resume job |
| DELETE | `/api/extract/job/{id}` | Delete job metadata |
| GET | `/api/extract/jobs` | List all jobs |
| GET | `/api/files` | List exported files |
| GET | `/api/files/{name}/preview` | Head/tail preview |
| GET | `/api/files/{name}/download` | Download file |
| GET | `/api/files/{name}/summary` | Extraction summary for file |
| DELETE | `/api/files/{name}` | Delete file |
| POST | `/api/ingest/send` | Proxy to Dynatrace Logs API v2 |
| POST | `/api/ingest-config` | Save ingest config |
| GET | `/api/ingest-config` | Read ingest config |

---

## Docker Commands

```bash
# Build and start
docker compose build --no-cache && docker compose up

# Start in background
docker compose up -d

# Stop (keeps token/config/exports volume)
docker compose down

# Full reset (loses stored tokens and extracted files)
docker compose down -v --rmi all
```

---

## Release History

| Version | Date | Highlights |
|---------|------|------------|
| **v2.1** | 2026-04-08 | Smart extraction engine, pre-flight density profiling, dynamic caps, slot merging, parallel workers, pipe deadlock fix, grouped sidebar, extraction/ingest history, job management |
| v1.1 | 2025-10-xx | Initial public release — chunked extraction via SSE, query panel, ingest panel |

---

## Project Structure

```
grailkeeper/
├── Dockerfile
├── docker-compose.yml
├── backend/
│   ├── app.py                  # FastAPI: query, extract engine, ingest proxy, file mgmt
│   ├── entrypoint.py
│   └── requirements.txt
└── frontend/
    └── index.html              # Single-file UI — no framework, pure JS
```

---

## Dependencies & Attribution

### [dtctl](https://github.com/dynatrace-oss/dtctl)
Query and Extract panels use `dtctl` — the Dynatrace CLI for Grail data access. Grailkeeper wraps it with a browser UI and orchestration layer.

### [Logstreamity](https://justschwendi.github.io/logstreamity/)
The Ingest panel is inspired by Logstreamity by Christian Schwendemann. Grailkeeper reimplements the ingest modes (Sequential, Historic, Scattered) and attribute injection concept.

---

## Disclaimer

This project is not officially supported by Dynatrace. Independent open-source tool — use at your own risk and only against tenants you control.

---

## License

MIT — see [LICENSE](LICENSE)
