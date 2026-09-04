# Grailkeeper

![Version](https://img.shields.io/badge/version-4.0.18-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Docker](https://img.shields.io/badge/docker-ready-blue)

**Grailkeeper** is a pure-Python bulk extraction tool for the [Dynatrace Grail Query API](https://developer.dynatrace.com/develop/grail/). It runs as a Docker container with a dark-themed web UI and handles large-scale log exports that the Grail API was not originally designed for.

---

## Features

### Extraction Engine
- **Recursive time-window splitting** — automatically splits time windows that return truncated results, down to configurable max depth
- **Zero-copy streaming** — records flow from Grail → temp file → output file via disk; never loaded into RAM as Python lists (peak RAM ~57 MB regardless of response size)
- **Watchdog thread** — detects and recovers from HTTP hangs (Grail sends headers then stalls on large responses)
- **Configurable caps** — max records per window (default 500k), max split depth (default 10), initial window size
- **Preflight probe** — optional dry-run to count records before committing to a full extraction
- **Pause / Resume / Stop** — full job lifecycle control mid-extraction
- **Output formats** — JSONL, CSV, raw text
- **Auto job ID prefix** — output files automatically prefixed with job ID (`{jid8}_{name}.jsonl`) to avoid collisions

### Web UI (Dark Theme)
- **Extract** — configure DQL, timeframe, output options; live log streaming with depth-colored output
- **Jobs** — view all past and running jobs with stats, progress bar (Window X/Y · Z%), verify button, tree button, extract button, log button
- **Files** — instant file listing (cached record counts via `.meta` sidecar); background counting with live flash indicators; file preview, download, delete
- **Query** — interactive DQL query runner against the connected tenant
- **History** — past query history
- **Settings** — tenant URL, API token, connection test

### Job Features
- **⚡ Extract** — navigate to extraction page with all original settings restored (DQL, timeframe, caps, filename)
- **📋 Log** — fullscreen popup with the complete extraction log, color-coded identical to the live extraction view
- **🔍 Verify** — runs `| summarize count()` against the same timeframe and shows extracted vs Grail count with color-coded delta (green <1%, yellow <10%, red >10%) and a pulsing dot indicator
- **🌳 Tree** — fullscreen SVG split tree visualization: DQL box at top with timeframe, circles for each window/split node colored by window (W1/W2/W3) and labelled with depth (W1·D0, W1·D1...), date ranges, record counts, scanned MB, execution ms; zoom/pan/fit controls

### Files Page
- **Instant load** — never blocks on record counting
- **`counting…`** flash — grey pulsing text while background line count runs; updates in-place when done
- **`⏳ extracting…`** flash — cyan pulsing text for files belonging to running jobs
- **Cached counts** — stored in `.meta` sidecar files so subsequent loads are instant
- **Sequential background counting** — max 1 file counted at a time to avoid overwhelming the server

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Dynatrace tenant with Grail enabled
- API token with `storage:logs:read` and `storage:query:execute` scopes

### 1. Clone and Start

```bash
git clone https://github.com/fragelo/grailkeeper.git
cd grailkeeper
docker compose up --build
```

Open **http://localhost:8000** in your browser.

### 2. Configure

Go to **Settings**, enter:
- **Environment URL**: `https://{your-tenant}.apps.dynatrace.com`
- **API Token**: your Dynatrace API token

Click **Test Connection** to verify.

### 3. Extract

Go to **Extract**, set:
- **DQL**: e.g. `fetch logs | filter dt.entity.host == "HOST-xxx"`
- **Start / End**: extraction timeframe
- **Output filename**: base name (job ID is auto-prefixed)

Click **▼ Start Extraction**.

---

## Architecture

```
Browser ──► FastAPI (port 8000)
               │
               ├── extractor.py   ← recursive window splitting, job lifecycle
               ├── grail_engine.py ← Grail API client (POST→poll, streaming, watchdog)
               └── app.py         ← REST endpoints, file management, tree parser
```

### Data Flow (Zero-Copy)

```
Grail HTTP response  →  /tmp/xxx.json           (disk, streamed in chunks)
/tmp/xxx.json        →  /tmp/xxx.records.jsonl  (disk, ijson Pass 1)
/tmp/xxx.records.jsonl  →  exports/{jid}_{name}.jsonl  (disk, direct append)
```

Peak RAM: **~57 MB** regardless of response size or number of windows.

### Grail API Behaviour

- Grail returns ~1–1.5 GB per response packet (~500k records)
- `fetchTimeoutSeconds: 240` keeps queries in Grail's guaranteed zone (<5 min)
- `200 None` (empty body) = undocumented Grail behaviour on busy scheduler → window is retried
- `503 BUSY_SCHEDULER` = max concurrent queries reached → retried with backoff
- `enforceQueryConsumptionLimit` is intentionally omitted (causes HTTP 400 on some tenants)

---

## Configuration

All config is stored in `/root/.grailkeeper/config.json` inside the container (Docker volume `grailkeeper-data`).

| Field | Description |
|-------|-------------|
| `env_url` | Dynatrace environment URL |
| `api_token` | API token |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/extract/start` | Start a new extraction job |
| `GET` | `/api/extract/jobs` | List all jobs |
| `GET` | `/api/extract/job/{jid}` | Get job state + live stats |
| `GET` | `/api/extract/job/{jid}/log` | Get job log entries |
| `GET` | `/api/extract/job/{jid}/tree` | Get split tree for visualization |
| `POST` | `/api/extract/job/{jid}/pause` | Pause a running job |
| `POST` | `/api/extract/job/{jid}/resume` | Resume a paused job |
| `POST` | `/api/extract/job/{jid}/stop` | Stop a job |
| `DELETE` | `/api/extract/job/{jid}` | Delete a job |
| `POST` | `/api/query/run` | Run an ad-hoc DQL query (used by Verify) |
| `GET` | `/api/files` | List export files (instant, never blocks) |
| `POST` | `/api/files/{name}/count` | Trigger background record count |
| `GET` | `/api/files/{name}/count` | Get cached record count |
| `GET` | `/api/files/{name}/preview` | Preview first N lines |
| `GET` | `/api/files/{name}/download` | Download file |
| `DELETE` | `/api/files/{name}` | Delete file |
| `GET` | `/api/health` | Health check |
| `GET` | `/api/settings` | Get settings |
| `POST` | `/api/settings` | Save settings |

---

## Docker

```yaml
# docker-compose.yml
services:
  grailkeeper:
    build: .
    container_name: grailkeeper
    ports:
      - "8000:8000"
    volumes:
      - grailkeeper-data:/root/.grailkeeper
    restart: unless-stopped

volumes:
  grailkeeper-data:
```

No `mem_limit` is set — the container uses the full memory allocated to Docker/OrbStack.

---

## Version History

| Version | Highlights |
|---------|-----------|
| **4.0.18** | Timeframe shown in job card DQL line |
| **4.0.17** | Fix `_fileCountPoller` ReferenceError |
| **4.0.16** | Log modal uses same rich rendering as extraction page |
| **4.0.15** | Fix broken regex in log modal |
| **4.0.14** | Fix missing `<script>` tag; global JS error banner |
| **4.0.13** | 📋 Log popup modal per job; ⚡ Extract button rename |
| **4.0.12** | Fix extra `}}` JS syntax error causing full page freeze |
| **4.0.11** | Files page instant load; background counting; flash indicators |
| **4.0.9** | File record count caching (`.meta` sidecar) |
| **4.0.8** | Fix log race condition in jumpToExtraction |
| **4.0.7** | Fix nav auto-reconnect IIFE placement; W1·D0 tree badges |
| **4.0.6** | Remove dead viewJobLog; fix jumpToExtraction race |
| **4.0.5** | Fix treeFit using viewBox; circle node labels with full dates |
| **4.0.4** | Fix extract page vibration; tree window colors + W1/D0 badges |
| **4.0.3** | Fix duplicate logs; tree redesigned with circles and DQL box |
| **4.0.2** | Fix tree `_load_log`; auto job ID filename prefix; collision prevention |
| **4.0.1** | Fix `windows_current` merge; fix duplicate log on navigate-back |
| **4.0.0** | 🌳 Split Tree visualization |
| **3.8.23** | File `.meta` sidecar for instant record counts |
| **3.8.22** | File collision prevention; job_id in file listing; checkbox on jump |
| **3.8.21** | Clickable filename → Files tab; jumpToExtraction restores all settings |
| **3.8.20** | Fix verify count string→number parsing |
| **3.8.18** | Pulsing dot on verify result (green/yellow/red) |
| **3.8.17** | Progress bar on extraction page and job cards |
| **3.8.15** | Fix verify count DQL regex (preserve filter pipes) |
| **3.8.13** | True zero-copy streaming (Pass 3 eliminated); 🔍 Verify button |
| **3.8.11** | Fix metadata display (scannedBytes/Records/Ms paths); remove mem_limit |
| **3.8.9** | Remove enforceQueryConsumptionLimit (400 on some tenants) |
| **3.8.8** | ijson two-pass streaming parser |
| **3.8.7** | Watchdog starts before GET; session close on hang |
| **3.8.3** | Initial streaming + watchdog architecture |

---

## License

MIT
