# Grailkeeper

**Dynatrace Grail Query, Extract & Ingest UI** — a browser-based interface for querying, extracting and ingesting data from Dynatrace Grail, powered by the `dtctl` CLI running inside a Docker container.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Docker](https://img.shields.io/badge/docker-required-blue.svg)
![dtctl](https://img.shields.io/badge/dtctl-v0.16.0-cyan.svg)
![Version](https://img.shields.io/badge/version-1.1-green.svg)

---

## Features

| Panel | Description |
|-------|-------------|
| ⚡ **Query** | DQL editor with all dtctl parameters — Ctrl+Enter to run, live console, write to disk, table/raw toggle, CSV/content/raw export |
| ⛏ **Extract** | Chunked extraction for millions of records — date pickers, quick ranges (7d–365d), progress bar, ETA, resume if stopped |
| 📥 **Ingest** | Log file ingest via Dynatrace Logs API v2 — Sequential, Historic and Scattered modes, file upload, 5 built-in demo datasets, attribute injection |
| 📜 **History** | Auto-records every DQL query — replay with one click, edit in editor, delete entries, reset all |
| 📁 **Files** | Preview and download all exported files from the container |
| 🔑 **Configure** | Two separate token configs: Platform Token for Query/Extract, Access Token for Ingest |

### Proven at scale

> 3,701,760 records extracted · 5.18 GB JSONL output · 724 chunks · ~5 minutes · from a tenant with 81M total log records

---

## Architecture

```
Browser (localhost:8000)
    ↕ HTTP / SSE
FastAPI Backend (/app/backend/app.py)
    ↕ subprocess
dtctl v0.16.0 (Linux amd64, /usr/local/bin/dtctl)
    ↕ HTTPS
Dynatrace Grail API (*.live.dynatrace.com)
```

Everything runs inside a single Docker container (`grailkeeper`) with a persistent volume for token/context storage.

---

## Requirements

- Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- `docker compose` v2+
- 16 GB+ RAM recommended (container uses up to 20 GB for large extractions)
- Internet access on first build (downloads dtctl binary from GitHub)

---

## Quick Start

### Option A — Setup script (recommended)

```bash
chmod +x setup-grailkeeper.sh
./setup-grailkeeper.sh
cd grailkeeper
docker compose up --build
```

### Option B — From the `grailkeeper/` folder

```bash
cd grailkeeper
docker compose up --build
```

Open **http://localhost:8000** in your browser.

---

## Configuration

Go to **Configure** after startup:

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

Required scope:
```
logs.ingest
```

> **Note:** The ingest token uses the classic API endpoint `*.live.dynatrace.com/api/v2/logs/ingest`. Grailkeeper automatically converts `.apps.dynatrace.com` URLs to `.live.dynatrace.com` for the ingest call.

---

## Chunked Extraction

The Extract panel splits a date range into hourly (or custom) chunks and runs one `dtctl` query per chunk, appending results to a single JSONL file. This safely handles tenants with millions of records without memory issues.

```
DQL:   fetch logs, scanLimitGBytes:-1 | limit 50000000
Range: 2025-09-16 → 2026-03-15  (6 months)
Chunk: 6 hours  →  724 total calls
Max:   500,000 records per chunk
```

**Resume support** — if extraction is stopped, restart with the same filename to continue from the last record.

---

## Log Ingest Modes

| Mode | Description | Best for |
|------|-------------|----------|
| **Sequential** | Real-time playback with configurable delay between lines | Live troubleshooting demos |
| **Historic** | Fast ingest with planned timestamps from a chosen start time | Retrofitting entities with logs |
| **Scattered** | Distribute timestamps across a time window with jitter | Simulating hours of logs in seconds |

Built-in demo datasets: Apache Access Log, Linux Syslog, App JSON, Security Events, CDR Telecom.

---

## Docker Commands

```bash
# Build and start
docker compose up --build

# Start in background
docker compose up -d

# Stop (keeps token/config volume)
docker compose down

# Full reset (loses stored tokens)
docker compose down -v --rmi all
```

---

## Project Structure

```
grailkeeper/
├── Dockerfile                  # Python 3.11-slim + dtctl v0.16.0
├── docker-compose.yml          # Service definition, port 8000, mem_limit 20g
├── backend/
│   ├── app.py                  # FastAPI: query, extract, ingest proxy, file mgmt
│   ├── entrypoint.py           # uvicorn launcher
│   └── requirements.txt        # fastapi, uvicorn, pydantic, requests
└── frontend/
    └── index.html              # Single-file UI (no framework, pure JS)
```

---

## API Endpoints (backend)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Connection status + dtctl version |
| POST | `/api/config` | Save dtctl Platform Token config |
| POST | `/api/command/stream` | SSE stream — run dtctl query |
| POST | `/api/extract/chunked` | SSE stream — chunked extraction loop |
| POST | `/api/ingest/send` | Proxy log events to Dynatrace Logs API v2 |
| POST | `/api/ingest-config` | Save ingest Access Token config |
| GET | `/api/ingest-config` | Read ingest config |
| GET | `/api/files` | List exported files |
| GET | `/api/files/{name}/preview` | Preview last N lines |
| GET | `/api/files/{name}/download` | Download file |
| DELETE | `/api/files/{name}` | Delete file |
| POST | `/api/export/full` | Export result as full CSV |
| POST | `/api/export/content` | Export content field as .log |

---

## Dependencies & Attribution

Grailkeeper is built on top of two independent open-source projects. Changes to either may impact Grailkeeper's functionality.

### [dtctl](https://github.com/dynatrace-oss/dtctl)
The Query and Extract panels use `dtctl` — the Dynatrace CLI for Grail data access — as the underlying engine. All DQL queries, async polling, chunked extraction and output formatting are handled by `dtctl` running inside the container. Grailkeeper simply wraps it with a browser UI and orchestration layer.

> If `dtctl` changes its CLI flags, authentication model, or output format, the Query and Extract panels may be affected.

### [Logstreamity](https://justschwendi.github.io/logstreamity/)
The Ingest panel is inspired by [Logstreamity](https://justschwendi.github.io/logstreamity/) by Christian Schwendemann — a client-side log ingest playground for Dynatrace Logs API v2. Grailkeeper reimplements the core ingest modes (Sequential, Historic, Scattered) and attribute injection concept, but routes calls through the backend to avoid browser CORS restrictions.

> If the Dynatrace Logs API v2 endpoint or authentication model changes, the Ingest panel may be affected.

---

## Disclaimer

This project is not officially supported by Dynatrace. It is an independent open-source tool built on top of `dtctl` and inspired by Logstreamity. Use at your own risk and only against Dynatrace tenants you control.

---

## License

MIT — see [LICENSE](LICENSE)
