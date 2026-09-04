"""
app.py — Grailkeeper v3.0 backend
Pure Python FastAPI. No dtctl dependency.
"""

import os, json, uuid, threading, logging
from datetime        import datetime, timezone
from typing          import Optional
from fastapi          import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic         import BaseModel

import grail_engine as ge
import extractor    as ex

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
log = logging.getLogger("app")

VERSION = "4.0.18"

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR  = "/root/.grailkeeper"
JOBS_DIR  = os.path.join(DATA_DIR, "jobs")
EXP_DIR   = os.path.join(DATA_DIR, "exports")

def _meta_path(fname): return os.path.join(EXP_DIR, fname + ".meta")

def _read_meta(fname):
    try:
        import json as _j
        with open(_meta_path(fname)) as f: return _j.load(f)
    except Exception: return {}

def _write_meta(fname, data):
    try:
        import json as _j
        with open(_meta_path(fname), 'w') as f: _j.dump(data, f)
    except Exception: pass
CFG_FILE  = os.path.join(DATA_DIR, "config.json")

for d in [DATA_DIR, JOBS_DIR, EXP_DIR]:
    os.makedirs(d, exist_ok=True)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Grailkeeper v3.0")
app.mount("/static", StaticFiles(directory="/app/static"), name="static")

# ── In-memory job store ────────────────────────────────────────────────────────
_jobs: dict[str, ex.ExtractionJob] = {}
_jobs_lock = threading.Lock()

# ── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    "env_url":       "",
    "token":         "",
    "max_records":   500_000,
    "max_depth":     10,
    "initial_hours": 24,   # 0 = full range
    "default_probe": False,
    "default_fields": "content",  # for raw format
}

def load_config() -> dict:
    if os.path.exists(CFG_FILE):
        try:
            with open(CFG_FILE) as f:
                saved = json.load(f)
            cfg = {**DEFAULT_SETTINGS, **saved}
            return cfg
        except Exception:
            pass
    return dict(DEFAULT_SETTINGS)

def save_config(cfg: dict):
    with open(CFG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

# ── Job log helpers ────────────────────────────────────────────────────────────
def _log_path(jid: str) -> str:
    return os.path.join(JOBS_DIR, f"{jid}.log.jsonl")

def _state_path(jid: str) -> str:
    return os.path.join(JOBS_DIR, f"{jid}.state.json")

def append_log(jid: str, entry: dict):
    try:
        with open(_log_path(jid), "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

def read_log(jid: str, tail: int = 200) -> list:
    path = _log_path(jid)
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            lines = f.readlines()
        return [json.loads(l) for l in lines[-tail:] if l.strip()]
    except Exception:
        return []

def write_state(jid: str, state: dict):
    tmp = _state_path(jid) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, _state_path(jid))

def read_state(jid: str) -> Optional[dict]:
    path = _state_path(jid)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None

# ── Startup ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    log.info(f"[startup] Grailkeeper v{VERSION} starting")
    cfg = load_config()
    if cfg.get("env_url") and cfg.get("token"):
        ge.set_credentials(cfg["env_url"], cfg["token"])
        log.info(f"[startup] Loaded credentials for {cfg['env_url'][:50]}")
    else:
        log.info("[startup] No credentials configured yet")

@app.get("/api/version")
def get_version():
    return {"version": VERSION}

# ── Frontend ───────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    from fastapi.responses import Response
    with open("/app/frontend/index.html", encoding="utf-8") as f:
        html = f.read()
    return Response(content=html, media_type="text/html",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate",
                             "Pragma": "no-cache", "Expires": "0"})

# ── Config endpoints ───────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    cfg = load_config()
    # Mask token
    masked = {**cfg, "token": "***" if cfg.get("token") else ""}
    return masked

class ConfigRequest(BaseModel):
    env_url:        str
    token:          str
    max_records:    int  = 500_000
    max_depth:      int  = 10
    initial_hours:  int  = 24
    default_probe:  bool = False
    default_fields: str  = "content"

@app.post("/api/config")
def post_config(req: ConfigRequest):
    cfg = load_config()
    token = cfg.get("token", "") if req.token == "__KEEP__" else req.token
    cfg.update({
        "env_url":        req.env_url.rstrip("/"),
        "token":          token,
        "max_records":    req.max_records,
        "max_depth":      req.max_depth,
        "initial_hours":  req.initial_hours,
        "default_probe":  req.default_probe,
        "default_fields": req.default_fields,
    })
    save_config(cfg)
    ge.set_credentials(cfg["env_url"], cfg["token"])
    return {"success": True}

# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/api/debug/job/{jid}")
def debug_job(jid: str):
    """Return raw state file content for debugging."""
    state = read_state(jid)
    if not state:
        return {"error": "not found"}
    return state

@app.post("/api/query/run")
def verify_query(body: dict):
    """Run a DQL query and return first record — used for verify count()."""
    if not ge.has_credentials():
        return {"error": "Credentials not configured", "records": []}
    try:
        result = ge.query(
            dql         = body.get("dql", ""),
            start       = body.get("start", ""),
            end         = body.get("end", ""),
            max_records = 1,
            strip_limit = False,
            stop_flag   = None,
        )
        # For summarize count() the result comes back in records_jsonl, not records list
        # Read the first line of the JSONL file if present
        records = result.get("records", [])
        jsonl_path = result.get("records_jsonl")
        if not records and jsonl_path:
            import os, json as _json
            try:
                with open(jsonl_path, 'r') as f:
                    line = f.readline().strip()
                    if line:
                        records = [_json.loads(line)]
            finally:
                if os.path.exists(jsonl_path):
                    os.unlink(jsonl_path)
        return {"records": records, "count": result.get("count", 0)}
    except Exception as e:
        return {"error": str(e), "records": []}



@app.get("/api/extract/job/{jid}/tree")
def job_tree(jid: str):
    """Parse job log into a split tree structure for visualization."""
    import re as _re
    entries = read_log(jid, tail=9999)
    if not entries:
        return {"error": "Job not found", "nodes": []}

    # Parse log lines into node events
    nodes = []
    node_stack = []  # stack of open node indices
    node_id = [0]

    depth_re   = _re.compile(r'\[depth=(\d+)\]\s+(.*?)\s+→\s+(.*?)\s+\((\d+\.\d+)h\)')
    result_re  = _re.compile(r'→\s+([\d,]+)\s+records.*?scanned\s+([\d,]+)MB.*?(\d+)ms.*?truncated=(True|False)')
    split_re   = _re.compile(r'↕\s+Split')
    written_re = _re.compile(r'✅\s+Written\s+([\d,]+)\s+records')
    empty_re   = _re.compile(r'○\s+Empty\s+window')
    warn_re    = _re.compile(r'⚠️.*Max depth')

    def new_node(depth, start, end, hours):
        nid = node_id[0]; node_id[0] += 1
        return {"id": nid, "depth": depth, "start": start, "end": end,
                "hours": float(hours), "records": 0, "scanned_mb": 0,
                "exec_ms": 0, "truncated": False, "outcome": "pending",
                "children": [], "parent": None}

    root_nodes = []
    open_nodes = {}  # depth -> current open node at that depth

    for e in entries:
        msg = e.get("msg", "")

        m = depth_re.search(msg)
        if m:
            depth, start, end, hours = int(m.group(1)), m.group(2), m.group(3), m.group(4)
            node = new_node(depth, start, end, hours)
            open_nodes[depth] = node
            # Connect to parent
            if depth > 0 and (depth-1) in open_nodes:
                parent = open_nodes[depth-1]
                node["parent"] = parent["id"]
                parent["children"].append(node["id"])
            if depth == 0:
                root_nodes.append(node["id"])
            nodes.append(node)
            continue

        if open_nodes:
            cur_depth = max(open_nodes.keys())
            node = open_nodes.get(cur_depth)
            if node:
                m = result_re.search(msg)
                if m:
                    node["records"]    = int(m.group(1).replace(",",""))
                    node["scanned_mb"] = int(m.group(2).replace(",",""))
                    node["exec_ms"]    = int(m.group(3))
                    node["truncated"]  = m.group(4) == "True"
                    continue
                if split_re.search(msg):
                    node["outcome"] = "split"
                    continue
                m = written_re.search(msg)
                if m:
                    node["outcome"] = "written"
                    node["records"] = int(m.group(1).replace(",",""))
                    open_nodes.pop(cur_depth, None)
                    continue
                if empty_re.search(msg):
                    node["outcome"] = "empty"
                    open_nodes.pop(cur_depth, None)
                    continue
                if warn_re.search(msg):
                    node["outcome"] = "maxdepth"
                    continue

    state = read_state(jid) or {}
    return {
        "job_id":  jid,
        "dql":     state.get("dql",""),
        "start":   state.get("start",""),
        "end":     state.get("end",""),
        "nodes":   nodes,
        "roots":   root_nodes,
    }

@app.get("/api/health")
def health():
    if not ge.has_credentials():
        return {"status": "unconfigured"}
    result = ge.health_check()
    return {
        "status":   "ok" if result["ok"] else "error",
        "detail":   result.get("error", ""),
        "query_id": result.get("query_id", ""),
    }


# ── Query endpoint ─────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    dql:         str
    start:       str
    end:         str
    max_records: int = 1000

@app.post("/api/query")
def run_query(req: QueryRequest):
    if not ge.has_credentials():
        raise HTTPException(400, "Credentials not configured")
    max_rec = min(req.max_records, 500_000)  # hard cap — never exceed Grail limit
    try:
        result = ge.query(
            dql         = req.dql,
            start       = req.start,
            end         = req.end,
            max_records = max_rec,
        )
        # Save to history
        save_history_entry({
            "type":       "query",
            "dql":        req.dql,
            "start":      req.start,
            "end":        req.end,
            "records":    result["count"],
            "truncated":  result["truncated"],
            "elapsed_ms": result["execution_ms"],
            "ts":         datetime.now(timezone.utc).isoformat(),
        })
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

# ── Extract endpoints ──────────────────────────────────────────────────────────
class ExtractRequest(BaseModel):
    dql:           str
    start:         str
    end:           str
    filename:      str
    fmt:           str  = "jsonl"   # "jsonl", "csv", or "raw"
    fields:        str  = "content" # for raw format: comma-separated field names
    max_records:   Optional[int] = None
    max_depth:     Optional[int] = None
    initial_hours: Optional[int] = None
    probe:         bool = False     # pre-flight binary search profiling
    scheduled_at:  Optional[str] = None  # ISO datetime to delay start

@app.post("/api/extract/start")
def start_extraction(req: ExtractRequest):
    if not ge.has_credentials():
        raise HTTPException(400, "Credentials not configured")

    cfg = load_config()
    jid = uuid.uuid4().hex[:8]

    # Parse dates
    def parse_dt(s: str) -> datetime:
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse date: {s}")

    start_dt = parse_dt(req.start)
    end_dt   = parse_dt(req.end)

    ext = {"csv": "csv", "raw": "txt", "jsonl": "jsonl"}.get(req.fmt, "jsonl")
    # Strip any existing jid prefix the user may have typed (idempotent)
    raw_name = req.filename.strip()
    if raw_name.startswith(jid[:8]+'_'):
        raw_name = raw_name[len(jid[:8])+1:]
    base_name = raw_name if raw_name.endswith(f".{ext}") else f"{raw_name}.{ext}"
    fname   = f"{jid[:8]}_{base_name}"
    outpath = os.path.join(EXP_DIR, fname)
    # Collision prevention: if file already exists add counter suffix
    if os.path.exists(outpath):
        stem, ext2 = os.path.splitext(base_name)
        fname   = f"{jid[:8]}_{stem}_2{ext2}"
        outpath = os.path.join(EXP_DIR, fname)

    # Auto-inject scanLimitGBytes:-1 if not already present
    import re as _re
    dql_final = req.dql
    if not _re.search(r'scanLimitGBytes', dql_final, _re.IGNORECASE):
        dql_final = _re.sub(r'(fetch\s+\w+)', r'\1, scanLimitGBytes:-1', dql_final, count=1, flags=_re.IGNORECASE)

    job = ex.ExtractionJob(
        jid           = jid,
        dql           = dql_final,
        start         = start_dt,
        end           = end_dt,
        outpath       = outpath,
        fmt           = req.fmt,
        max_records   = req.max_records   or cfg["max_records"],
        max_depth     = req.max_depth     or cfg["max_depth"],
        initial_hours = req.initial_hours if req.initial_hours is not None
                        else cfg["initial_hours"],
        append_log    = append_log,
    )
    job.probe  = req.probe
    job.fields = [f.strip() for f in req.fields.split(",") if f.strip()]

    state = {
        "job_id":        jid,
        "status":        "scheduled" if req.scheduled_at else "running",
        "dql":           dql_final,
        "start":         req.start,
        "end":           req.end,
        "filename":      fname,
        "fmt":           req.fmt,
        "fields":        req.fields,
        "outpath":       outpath,
        "max_records":   job.max_records,
        "max_depth":     job.max_depth,
        "initial_hours": job.initial_hours,
        "probe":         req.probe,
        "started_at":    datetime.now(timezone.utc).isoformat(),
    }
    write_state(jid, state)

    with _jobs_lock:
        _jobs[jid] = job

    def _run():
        try:
            # Handle scheduled start
            if req.scheduled_at:
                target = datetime.fromisoformat(req.scheduled_at.replace('Z','+00:00'))
                now    = datetime.now(timezone.utc)
                wait_s = (target - now).total_seconds()
                if wait_s > 0:
                    log.info(f"Job {jid} scheduled — waiting {wait_s:.0f}s")
                    s = read_state(jid) or {}
                    s["status"] = "scheduled"
                    s["scheduled_at"] = req.scheduled_at
                    write_state(jid, s)
                    import time as _t
                    _t.sleep(wait_s)
                    if job.stopped():
                        return
                    s["status"] = "running"
                    write_state(jid, s)
            ex.run_extraction(job)
            # Final state update
            s = read_state(jid) or {}
            finished  = datetime.now(timezone.utc)
            elapsed_s = round((finished - job.started_at).total_seconds(), 1)
            s.update({
                "status":          "done" if not job.stopped() else "stopped",
                "total_records":   job.total_records,
                "total_api_calls": job.total_api_calls,
                "total_splits":    job.total_splits,
                "scanned_bytes":   job.total_scanned_b,
                "scanned_records": job.total_scanned_r,
                "errors":          job.errors,
                "warnings":        job.warnings,
                "finished_at":     finished.isoformat(),
                "elapsed_s":       elapsed_s,
            })
            write_state(jid, s)
            # Save to history server-side — survives browser close
            save_history_entry({
                "type":     "extract",
                "dql":      dql_final,
                "start":    req.start,
                "end":      req.end,
                "records":  job.total_records,
                "calls":    job.total_api_calls,
                "splits":   job.total_splits,
                "filename": fname,
                "status":   s["status"],
                "elapsed_s": elapsed_s,
                "job_id":   jid,
                "ts":       finished.isoformat(),
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            log.error(f"Job {jid} crashed: {e}\n{tb}")
            s = read_state(jid) or {}
            s["status"] = "error"
            s["error"]  = str(e)
            s["traceback"] = tb
            write_state(jid, s)
        finally:
            with _jobs_lock:
                _jobs.pop(jid, None)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return {"job_id": jid, "filename": fname}

@app.get("/api/extract/job/{jid}")
def get_job(jid: str):
    state = read_state(jid)
    if not state:
        raise HTTPException(404, f"Job {jid} not found")
    # Enrich with live stats if still running
    with _jobs_lock:
        job = _jobs.get(jid)
    if job:
        elapsed = round((datetime.now(timezone.utc) - job.started_at).total_seconds(), 1)
        # Reflect actual pause/stop state from job object
        if job.stop_flag.is_set():
            state["status"] = "stopping"
        elif job.pause_flag.is_set():
            state["status"] = "paused"
        else:
            state["status"] = "running"
        state["live"] = {
            "total_records":   job.total_records,
            "total_api_calls": job.total_api_calls,
            "total_splits":    job.total_splits,
            "scanned_bytes":   job.total_scanned_b,
            "scanned_records": job.total_scanned_r,
            "errors":          job.errors,
            "warnings":        job.warnings,
            "elapsed_s":       elapsed,
            "windows_total":   job.windows_total,
            "windows_done":    job.windows_done,
            "windows_current": job.windows_current,
        }
    # Surface error details
    if state.get("status") == "error" and state.get("traceback"):
        state["error_detail"] = state["traceback"][-500:]  # last 500 chars
    # Expose start/end as ISO strings for verify feature
    state["start_iso"] = state.get("start", "")
    state["end_iso"]   = state.get("end", "")
    return state

@app.get("/api/extract/job/{jid}/log")
def get_job_log(jid: str, tail: int = 200):
    return {"entries": read_log(jid, tail)}

@app.post("/api/extract/job/{jid}/pause")
def pause_job(jid: str):
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job:
        raise HTTPException(404, f"Job {jid} not running")
    job.pause_flag.set()
    # Note: we do NOT cancel the current query for pause
    # The current window completes naturally (data preserved), then
    # the pause flag is checked before starting the next window
    s = read_state(jid) or {}
    s["status"] = "paused"
    write_state(jid, s)
    return {"job_id": jid, "action": "paused"}

@app.post("/api/extract/job/{jid}/resume")
def resume_job(jid: str):
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job:
        raise HTTPException(404, f"Job {jid} not running")
    job.pause_flag.clear()
    s = read_state(jid) or {}
    s["status"] = "running"
    write_state(jid, s)
    return {"job_id": jid, "action": "resumed"}

@app.post("/api/extract/job/{jid}/stop")
def stop_job(jid: str):
    with _jobs_lock:
        job = _jobs.get(jid)
    if job:
        job.stop_flag.set()
        job.cancel_current_query()  # cancel in-flight Grail query immediately
        s = read_state(jid) or {}
        s["status"] = "stopping"
        write_state(jid, s)
        return {"job_id": jid, "action": "stop_requested"}
    raise HTTPException(404, f"Job {jid} not running")

@app.delete("/api/extract/job/{jid}")
def delete_job(jid: str):
    # Stop if running
    with _jobs_lock:
        job = _jobs.get(jid)
    if job:
        job.stop_flag.set()
    # Remove state + log files
    for path in [_state_path(jid), _log_path(jid)]:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    return {"deleted": jid}

@app.get("/api/extract/jobs")
def list_jobs():
    jobs = []
    for fname in os.listdir(JOBS_DIR):
        if fname.endswith(".state.json"):
            jid = fname.replace(".state.json", "")
            s   = read_state(jid)
            if s:
                jobs.append(s)
    jobs.sort(key=lambda x: x.get("started_at", ""), reverse=True)
    return {"jobs": jobs}

# ── History endpoints ──────────────────────────────────────────────────────────
HISTORY_FILE = os.path.join(DATA_DIR, "history.json")

def load_history() -> list:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_history_entry(entry: dict):
    history = load_history()
    history.insert(0, entry)
    history = history[:200]  # keep last 200
    tmp = HISTORY_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(history, f, indent=2)
    os.replace(tmp, HISTORY_FILE)

@app.get("/api/history")
def get_history():
    return {"entries": load_history()}

@app.post("/api/history")
def add_history(entry: dict):
    save_history_entry(entry)
    return {"ok": True}

@app.delete("/api/history")
def clear_history():
    if os.path.exists(HISTORY_FILE):
        os.remove(HISTORY_FILE)
    return {"ok": True}

# ── Files endpoints ────────────────────────────────────────────────────────────
# Track files currently being counted in background
_counting_files: set = set()

@app.get("/api/files")
def list_files():
    """Return files instantly — never blocks on line counting.
    record_state: 'known' | 'counting' | 'extracting'
    """
    # Build filename→job map (running jobs = still extracting)
    fname_to_job = {}
    running_files = set()
    try:
        for jf in os.listdir(JOBS_DIR):
            if not jf.endswith(".log.jsonl"):
                continue
            jid_f = jf.replace(".log.jsonl", "")
            st = read_state(jid_f)
            if st and st.get("filename"):
                fname_to_job[st["filename"]] = jid_f
                if st.get("status") in ("running", "paused", "stopping"):
                    running_files.add(st["filename"])
    except Exception:
        pass

    files = []
    for fname in os.listdir(EXP_DIR):
        path = os.path.join(EXP_DIR, fname)
        try:
            if not os.path.isfile(path) or fname.endswith('.meta'):
                continue
            sz = os.path.getsize(path)
            meta = _read_meta(fname)
            job_id = meta.get("job_id") or fname_to_job.get(fname, "")

            if fname in running_files:
                record_state = "extracting"
                records = meta.get("records", 0)  # show what we have so far
            elif "records" in meta:
                record_state = "known"
                records = meta["records"]
            else:
                record_state = "counting"
                records = None

            files.append({
                "filename":     fname,
                "size_mb":      round(sz / 1024 / 1024, 2),
                "records":      records,
                "record_state": record_state,
                "modified":     datetime.fromtimestamp(
                                    os.path.getmtime(path), tz=timezone.utc).isoformat(),
                "job_id":       job_id,
            })
        except FileNotFoundError:
            pass
    files.sort(key=lambda x: x["modified"], reverse=True)
    return {"files": files}


@app.post("/api/files/{filename}/count")
def count_file_records(filename: str):
    """Count lines in a file and cache to .meta — call once per uncounted file."""
    import threading
    path = os.path.join(EXP_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")
    if filename in _counting_files:
        return {"status": "already_counting"}
    meta = _read_meta(filename)
    if "records" in meta:
        return {"status": "already_known", "records": meta["records"]}
    # Limit to 1 concurrent count to avoid overwhelming FastAPI workers
    if len(_counting_files) >= 1:
        return {"status": "busy", "message": "Another file is being counted, retry shortly"}

    def _count():
        _counting_files.add(filename)
        try:
            with open(path) as f:
                n = sum(1 for _ in f)
            m = _read_meta(filename)
            m["records"] = n
            _write_meta(filename, m)
        except Exception:
            pass
        finally:
            _counting_files.discard(filename)

    threading.Thread(target=_count, daemon=True).start()
    return {"status": "counting_started"}


@app.get("/api/files/{filename}/count")
def get_file_count(filename: str):
    """Return cached record count if available."""
    meta = _read_meta(filename)
    if "records" in meta:
        return {"status": "known", "records": meta["records"]}
    if filename in _counting_files:
        return {"status": "counting"}
    return {"status": "unknown"}

@app.get("/api/files/{filename}/preview")
def preview_file(filename: str, n: int = 5):
    path = os.path.join(EXP_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")
    lines = []
    is_csv  = filename.endswith(".csv")
    is_jsonl = filename.endswith(".jsonl")
    try:
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                raw_line = line.strip()
                if not raw_line:
                    continue
                if len(lines) >= n:
                    break
                if is_jsonl:
                    try:
                        lines.append(json.loads(raw_line))
                    except Exception:
                        lines.append({"line": raw_line})
                else:
                    # CSV or raw text — return as plain string
                    lines.append({"line": raw_line})
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"records": lines, "fmt": "csv" if is_csv else ("jsonl" if is_jsonl else "raw")}

@app.get("/api/files/{filename}/download")
def download_file(filename: str):
    path = os.path.join(EXP_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")
    return FileResponse(path, filename=filename,
                        media_type="application/octet-stream")

@app.delete("/api/files/{filename}")
def delete_file(filename: str):
    path = os.path.join(EXP_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")
    os.remove(path)
    return {"deleted": filename}
