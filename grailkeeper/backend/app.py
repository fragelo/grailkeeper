from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, Response
from pydantic import BaseModel
import subprocess, threading, queue, json, csv, io, os, re, uuid, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DTCTL    = "/usr/local/bin/dtctl"
OUTDIR   = "/root/.config/dtctl/exports"
JOBS_DIR = "/root/.config/dtctl/jobs"
os.makedirs(OUTDIR,   exist_ok=True)
os.makedirs(JOBS_DIR, exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
SAFETY_FACTOR   = 3.0
MIN_RECORDS_CAP = 500_000               # floor: 500k
MAX_RECORDS_CAP = 50_000_000           # ceiling: 50M — handles 73M/h dense telecom
MIN_BYTES_CAP   = 200 * 1024 * 1024    # 200MB floor
MAX_BYTES_CAP   = 5 * 1024 * 1024 * 1024   # 5GB ceiling — safe for 20GB container
MIN_WINDOW_SECS = 60                    # 1 minute minimum — dense tenants need this
AGGREGATE_CMDS  = ["summarize", "timeseries", "makeresults"]

# ── Utilities ─────────────────────────────────────────────────────────────────
def fmt_ms(dt: datetime) -> str:
    ms = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

DEBUG_KEYS = ["===> REQUEST","===> RESPONSE","STATUS:","TIME:","POST https://",
              "GET https://",'"state":','"progress":','"requestToken":',
              '"scannedRecords":','"scannedBytes":','"executionTimeMilliseconds":',
              "Using config","RESULT_TRUNCATED","SCAN_LIMIT"]

def clean_debug(line):
    if not any(k in line for k in DEBUG_KEYS): return None
    if "===> REQUEST"  in line: return "===> REQUEST ==="
    if "===> RESPONSE" in line: return "===> RESPONSE ==="
    if "RESULT_TRUNCATED" in line: return "⚠️ RESULT_TRUNCATED"
    if "SCAN_LIMIT"        in line: return "⚠️ SCAN_LIMIT_GBYTES"
    if line.startswith("STATUS:"): return line.strip()
    if line.startswith("TIME:"):   return line.strip()
    if line.startswith("POST https://") or line.startswith("GET https://"): return line.strip()
    if "Using config" in line: return line.strip()
    parts = []
    for k in ["state","progress","scannedRecords","scannedBytes","executionTimeMilliseconds"]:
        pattern = '"' + k + '":"?([^,"' + '}' + ']*)"?'
        m = re.search(pattern, line)
        if m: parts.append(f'{k}:{m.group(1).strip(chr(34))}')
    return " | ".join(parts) if parts else None

def run_dtctl(args):
    try:
        r = subprocess.run([DTCTL]+args, capture_output=True, text=True,
                           timeout=None, env={**os.environ})
        return {"stdout":r.stdout,"stderr":r.stderr,"returncode":r.returncode,"success":r.returncode==0}
    except FileNotFoundError:
        raise HTTPException(500,"dtctl not found")

# ── Job State ─────────────────────────────────────────────────────────────────
def job_state_path(jid):   return os.path.join(JOBS_DIR, f"{jid}.state.json")
def job_log_path(jid):     return os.path.join(JOBS_DIR, f"{jid}.log.jsonl")
def job_summary_path(jid): return os.path.join(JOBS_DIR, f"{jid}.summary.json")

def write_job_state(jid, state):
    """Write atomically — write to temp then rename to avoid corruption on kill."""
    p = job_state_path(jid)
    tmp = p + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, p)  # atomic on POSIX
    except Exception:
        # Fallback to direct write
        with open(p, "w") as f:
            json.dump(state, f, indent=2)

def read_job_state(jid) -> dict:
    p = job_state_path(jid)
    if not os.path.exists(p): return {}
    try:
        with open(p) as f:
            raw = f.read().strip()
        if not raw: return {}
        return json.loads(raw)
    except (json.JSONDecodeError, OSError, ValueError):
        return {}  # corrupt/empty file — treat as fresh

def append_job_log(jid, event: dict):
    event["ts"] = now_iso()
    with open(job_log_path(jid), "a") as f:
        f.write(json.dumps(event) + "\n")

def write_job_summary(jid, summary: dict):
    with open(job_summary_path(jid), "w") as f:
        json.dump(summary, f, indent=2)

def should_pause(jid): return read_job_state(jid).get("pause_requested", False)
def should_stop(jid):  return read_job_state(jid).get("stop_requested",  False)

def is_within_run_window(state: dict) -> bool:
    schedule = state.get("schedule")
    if not schedule: return True
    try:
        import pytz
        tz        = pytz.timezone(schedule.get("timezone","UTC"))
        now_local = datetime.now(tz)
        hour      = now_local.hour + now_local.minute / 60.0
        run_from  = schedule.get("run_from_hour", 0)
        run_to    = schedule.get("run_to_hour",  24)
        if run_from > run_to:
            return hour >= run_from or hour < run_to
        return run_from <= hour < run_to
    except Exception:
        return True

@app.on_event("startup")
def detect_interrupted():
    try:
        for f in os.listdir(JOBS_DIR):
            if not f.endswith(".state.json"): continue
            jid   = f.replace(".state.json","")
            state = read_job_state(jid)
            if not state: continue

            # Mark running/starting/preflight jobs as interrupted
            if state.get("status") in ("running","starting","preflight"):
                state["status"]           = "interrupted"
                state["interrupted_at"]   = now_iso()
                state["resume_available"] = True
                write_job_state(jid, state)
                append_job_log(jid, {"type":"interrupted","reason":"container_restart",
                                      "last_window":state.get("current_window_from","?")})

            # Ensure job_id is always stored in state (old files may lack it)
            if not state.get("job_id"):
                state["job_id"] = jid
                write_job_state(jid, state)

    except Exception as e:
        print(f"[startup] {e}")

# ── DQL Helpers ───────────────────────────────────────────────────────────────
def inject_timeframe(dql: str, from_str: str, to_str: str) -> str:
    dql = ' '.join(dql.split())
    dql = re.sub(r',?\s*from:"[^"]*"',  '', dql)
    dql = re.sub(r',?\s*to:"[^"]*"',    '', dql)
    dql = re.sub(r',?\s*from:[^\s,|"]+','', dql)
    dql = re.sub(r',?\s*to:[^\s,|"]+',  '', dql)
    for src in ["fetch logs","fetch events","fetch spans","fetch bizevents","fetch metrics"]:
        if src in dql:
            dql = dql.replace(src, f'{src}, from:"{from_str}", to:"{to_str}"')
            break
    return dql

def strip_for_preflight(dql: str) -> str:
    clean = re.sub(r'\|\s*limit\s+\d+',    '', dql, flags=re.IGNORECASE)
    clean = re.sub(r'\|\s*sort\s+[^\|]+',  '', clean, flags=re.IGNORECASE)
    return clean.strip().rstrip('|').strip()

def is_aggregated(dql: str) -> bool:
    return any(c in dql.lower() for c in AGGREGATE_CMDS)

def auto_interval(start_dt, end_dt) -> str:
    h = (end_dt - start_dt).total_seconds() / 3600 / 100
    if h <= 1:   return "1h"
    if h <= 6:   return "6h"
    if h <= 12:  return "12h"
    if h <= 24:  return "1d"
    if h <= 168: return "1w"
    return "4w"

def interval_hours(iv: str) -> float:
    return {"1h":1,"6h":6,"12h":12,"1d":24,"1w":168,"4w":672}.get(iv, 6)

def optimal_chunk_h(expected: float, avg_bytes: float, iv_h: float,
                    max_h: float = 168) -> float:
    """
    Compute ideal window duration in hours.
    expected:  records expected in iv_h hours
    avg_bytes: bytes per record
    iv_h:      preflight bin interval (e.g. 1h, 6h)
    max_h:     maximum allowed window (full date range)
    """
    if avg_bytes <= 0: avg_bytes = 500
    max_exp = min(MAX_RECORDS_CAP, MAX_BYTES_CAP / avg_bytes) / SAFETY_FACTOR
    density = expected / iv_h if iv_h > 0 else 0
    if density <= 0: return max_h  # no data — use maximum window
    ideal = max_exp / density  # hours that fit max_exp records at this density
    # Round DOWN to nearest clean boundary
    best = MIN_WINDOW_SECS / 3600  # absolute minimum
    for h in [MIN_WINDOW_SECS/3600, 0.25, 0.5, 1, 2, 3, 6, 12, 24, 48, 168]:
        if ideal >= h and h <= max_h: best = h
    return min(best, max_h)

def compute_caps(expected: float, avg_bytes: float) -> tuple:
    if avg_bytes <= 0: avg_bytes = 500
    r = max(MIN_RECORDS_CAP, min(int(expected * SAFETY_FACTOR), MAX_RECORDS_CAP))
    b = max(MIN_BYTES_CAP,   min(int(expected * avg_bytes * SAFETY_FACTOR), MAX_BYTES_CAP))
    return r, b

# ── Pre-flight ────────────────────────────────────────────────────────────────
def run_preflight(jid: str, dql: str, start_dt: datetime, end_dt: datetime) -> dict:
    append_job_log(jid, {"type":"preflight_start"})

    if is_aggregated(dql):
        append_job_log(jid, {"type":"preflight_skip","reason":"DQL already aggregated"})
        return {"skipped":True,"avg_bytes":500,"density":{},"interval":"6h","total_expected":0}

    iv       = auto_interval(start_dt, end_dt)
    from_str = fmt_ms(start_dt)
    to_str   = fmt_ms(end_dt)
    clean    = strip_for_preflight(dql)
    pdql     = inject_timeframe(clean, from_str, to_str)

    # Ensure timestamp available for bin()
    if re.search(r'\|\s*fields\b', pdql, re.IGNORECASE) and 'timestamp' not in pdql:
        pdql = re.sub(r'(\|\s*fields\b)', r'| fieldsAdd timestamp \1', pdql, count=1)

    pdql = f"{pdql} | summarize count(), by:bin(timestamp, {iv})"

    args = ["query", pdql,
            "--max-result-records", "5000",
            "--metadata", "scannedBytes,scannedRecords",
            "-o", "json",
            "--fetch-timeout-seconds", "300"]

    proc = subprocess.Popen([DTCTL]+args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env={**os.environ})
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        append_job_log(jid, {"type":"preflight_error","error":stderr.strip()[:300]})
        return {"skipped":True,"avg_bytes":500,"density":{},"interval":iv,"total_expected":0}

    try:
        # Log raw output to debug dtctl format
        append_job_log(jid, {"type":"preflight_raw",
                              "stdout_len": len(stdout),
                              "stdout_preview": stdout[:800].strip(),
                              "stderr_preview": stderr[:300].strip(),
                              "returncode": 0})

        parsed   = json.loads(stdout)

        # dtctl records location
        records = (parsed.get("records")
                   or parsed.get("result", {}).get("records")
                   or [])

        # dtctl metadata structure: { "metadata": { "scannedBytes": X, "scannedRecords": Y } }
        # NOT nested under "grail" — that is the SDK format, not dtctl CLI format
        raw_meta = parsed.get("metadata", {})
        meta     = (raw_meta.get("grail", {})   # SDK format
                    or raw_meta                  # dtctl CLI format (flat under metadata)
                    or {})
        sc_bytes = int(meta.get("scannedBytes",   0) or 0)
        sc_recs  = int(meta.get("scannedRecords", 0) or 0)
        avg_b    = (sc_bytes / sc_recs) if sc_recs > 0 else 500

        # Log parsed structure for debugging
        append_job_log(jid, {"type":"preflight_parsed",
                              "records_count": len(records),
                              "sc_recs": sc_recs,
                              "sc_bytes": sc_bytes,
                              "avg_b": round(avg_b, 1),
                              "first_record": records[0] if records else None,
                              "meta_keys": list(raw_meta.keys())[:10]})

        density = {}
        total   = 0
        for rec in records:
            # Key is "bin(timestamp, Xh)" — find it dynamically
            ts = ""
            for k, v in rec.items():
                if k.startswith("bin(") or k == "timestamp":
                    ts = str(v)
                    break
            # count() may be string or int
            cnt = 0
            for k, v in rec.items():
                if "count" in k.lower():
                    try: cnt = int(v)
                    except: cnt = 0
                    break
            if ts:
                density[ts] = cnt
            total += cnt

        # Log first few density keys to debug format
        sample_keys = list(density.keys())[:5]
        append_job_log(jid, {"type":"preflight_done","interval":iv,
                              "intervals":len(density),"total_expected":total,
                              "avg_bytes":round(avg_b,1),
                              "sample_keys":sample_keys,
                              "sample_counts":list(density.values())[:5]})
        # If density is empty but scannedRecords exists, log a warning
        if not density and sc_recs > 0:
            append_job_log(jid, {"type":"preflight_warn",
                                  "msg":"Density profile empty — bin query returned no records. "
                                        "Using scannedRecords as total estimate. "
                                        "Raw records sample: "+str([r.get("timestamp","?") for r in records[:3]])})
        return {"skipped":False,"avg_bytes":avg_b,"density":density,
                "interval":iv,"total_expected":total,
                "scanned_records":sc_recs,"scanned_bytes":sc_bytes}
    except Exception as e:
        append_job_log(jid, {"type":"preflight_error","error":str(e)})
        return {"skipped":True,"avg_bytes":500,"density":{},"interval":iv,"total_expected":0}

# ── Schedule Builder ──────────────────────────────────────────────────────────
def build_schedule(preflight: dict, start_dt: datetime, end_dt: datetime) -> list:
    iv       = preflight.get("interval","6h")
    density  = preflight.get("density", {})
    avg_b    = preflight.get("avg_bytes", 500)
    iv_h     = interval_hours(iv)
    windows  = []
    slot_s   = start_dt

    # If density is empty but we have total_expected, distribute evenly
    # This handles cases where DQL bin query returned no records (format mismatch)
    total_exp = preflight.get("total_expected", 0)
    if not density and total_exp > 0:
        total_hours = (end_dt - start_dt).total_seconds() / 3600
        n_slots = max(1, total_hours / iv_h)
        avg_per_slot = total_exp / n_slots
        t = start_dt
        while t < end_dt:
            density[fmt_ms(t)] = int(avg_per_slot)
            t += timedelta(hours=iv_h)

    # Build windows across the full range — merge sparse slots into larger windows
    total_range_h = (end_dt - start_dt).total_seconds() / 3600

    def get_density_for_slot(slot_s):
        """Get expected records for a slot from the density profile."""
        slot_key_t = fmt_ms(slot_s)[:13]
        slot_key_s = fmt_ms(slot_s)[:10]
        for k, v in density.items():
            k_clean = str(k).replace(" ", "T")[:19]
            if k_clean[:13] == slot_key_t or k_clean[:10] == slot_key_s:
                return int(v)
        return 0

    win_s = start_dt
    while win_s < end_dt:
        # Accumulate records across slots until we would exceed the cap
        accum_exp   = 0
        accum_end   = win_s
        slot_cursor = win_s

        while slot_cursor < end_dt:
            slot_exp = get_density_for_slot(slot_cursor)
            next_slot = min(slot_cursor + timedelta(hours=iv_h), end_dt)
            slot_h   = (next_slot - slot_cursor).total_seconds() / 3600
            # Check if adding this slot would exceed cap
            test_exp = accum_exp + slot_exp
            mr_test, mb_test = compute_caps(test_exp, avg_b)
            # Would this slot fit?
            if accum_exp > 0 and test_exp > mr_test / SAFETY_FACTOR:
                break  # window is full — stop accumulating
            accum_exp   += slot_exp
            accum_end    = next_slot
            slot_cursor  = next_slot
            # Hard ceiling: don't exceed total_range_h or 168h
            if (accum_end - win_s).total_seconds() / 3600 >= min(total_range_h, 168):
                break

        if accum_end <= win_s:
            accum_end = min(win_s + timedelta(hours=iv_h), end_dt)
        mr, mb = compute_caps(accum_exp, avg_b)
        windows.append({"start":win_s,"end":accum_end,"expected":int(accum_exp),
                         "max_records":mr,"max_bytes":mb})
        win_s = accum_end

    return windows

# ── Single Window Runner ──────────────────────────────────────────────────────
def run_window(dql: str, ws: datetime, we: datetime,
               mr: int, mb: int, extra: list,
               jid: str = None) -> dict:
    """
    Run dtctl for one time window.
    jid: if provided, polls pause/stop flags and kills dtctl immediately if set.
    """
    d = inject_timeframe(dql, fmt_ms(ws), fmt_ms(we))
    # Note: --max-result-bytes removed — it causes full scans on Grail
    # instead of stopping early. Records cap is sufficient for safety.
    args = ["query", d,
            "--max-result-records", str(mr),
            "--metadata", "scannedBytes,scannedRecords",
            "-o", "json", "--fetch-timeout-seconds", "600"] + extra

    # Write stdout to a temp file to avoid pipe buffer deadlock on large responses
    import tempfile
    tmp_out = tempfile.NamedTemporaryFile(mode='w', suffix='.json',
                                          delete=False, dir='/tmp')
    tmp_out.close()
    tmp_err = tempfile.NamedTemporaryFile(mode='w', suffix='.err',
                                          delete=False, dir='/tmp')
    tmp_err.close()

    try:
        with open(tmp_out.name, 'w') as fout, open(tmp_err.name, 'w') as ferr:
            proc = subprocess.Popen([DTCTL]+args, stdout=fout,
                                     stderr=ferr, env={**os.environ})

        # Poll for pause/stop every second
        if jid:
            while proc.poll() is None:
                try:
                    if should_stop(jid) or should_pause(jid):
                        proc.kill()
                        proc.wait()
                        return {"records":[],"count":0,"truncated":False,
                                "error":"interrupted","result_bytes":0,"interrupted":True}
                except Exception:
                    pass
                time.sleep(1)
        else:
            proc.wait()

        # Read results from temp files
        with open(tmp_out.name, 'r') as f:
            stdout = f.read()
        with open(tmp_err.name, 'r') as f:
            stderr = f.read()

        if proc.returncode != 0:
            return {"records":[],"count":0,"truncated":False,
                    "error":stderr.strip()[:400],"result_bytes":0}

        trunc = ("RESULT_TRUNCATED" in stderr or "SCAN_LIMIT" in stderr)
        try:
            parsed    = json.loads(stdout)
            recs      = parsed.get("records") or []
            count     = len(recs)
            res_bytes = len(stdout.encode())
            truncated = trunc or count >= mr
            return {"records":recs,"count":count,"truncated":truncated,
                    "error":None,"result_bytes":res_bytes,"interrupted":False}
        except Exception as e:
            return {"records":[],"count":0,"truncated":False,
                    "error":f"JSON parse error: {e} (stdout size: {len(stdout)} bytes)",
                    "result_bytes":0}
    finally:
        # Always clean up temp files
        for f in [tmp_out.name, tmp_err.name]:
            try: os.unlink(f)
            except: pass

# ── Recursive Extractor ───────────────────────────────────────────────────────
def extract_recursive(jid: str, dql: str,
                      ws: datetime, we: datetime,
                      mr: int, mb: int,
                      avg_b: float, outfile, extra: list, depth=0) -> dict:
    # Check for stop/pause at every recursion level — not just between top-level windows
    if should_stop(jid) or should_pause(jid):
        return {"records_written":0,"warnings":["interrupted_by_user"],"calls":0}

    result = run_window(dql, ws, we, mr, mb, extra, jid=jid)

    state = read_job_state(jid)
    state["api_calls"] = state.get("api_calls",0) + 1
    write_job_state(jid, state)

    if result.get("interrupted"):
        return {"records_written":0,"warnings":["interrupted_by_user"],"calls":0}

    if result["error"]:
        state["errors"] = state.get("errors",0) + 1
        write_job_state(jid, state)
        append_job_log(jid, {"type":"window_error","from":fmt_ms(ws),"to":fmt_ms(we),
                              "depth":depth,"error":result["error"]})
        return {"records_written":0,"warnings":[],"calls":1}

    if result["truncated"]:
        dur = (we - ws).total_seconds()
        if dur <= MIN_WINDOW_SECS * 2:
            # Cannot split further
            warn = f"⚠️ Min window {fmt_ms(ws)}→{fmt_ms(we)} still truncated. {result['count']} records written."
            state["warnings"] = state.get("warnings",0) + 1
            write_job_state(jid, state)
            append_job_log(jid, {"type":"truncation_warning","from":fmt_ms(ws),"to":fmt_ms(we),
                                  "depth":depth,"count":result["count"]})
            for rec in result["records"]:
                outfile.write(json.dumps(rec)+"\n")
            outfile.flush()
            return {"records_written":result["count"],"warnings":[warn],"calls":1}

        mid = ws + (we - ws) / 2
        # Pass parent caps unchanged — caps are already sized from density profile
        # Recalculating from truncated count degrades caps and causes cascading splits
        append_job_log(jid, {"type":"window_split","from":fmt_ms(ws),"to":fmt_ms(we),
                              "mid":fmt_ms(mid),"depth":depth})
        left  = extract_recursive(jid,dql,ws,mid, mr,mb,avg_b,outfile,extra,depth+1)
        right = extract_recursive(jid,dql,mid,we, mr,mb,avg_b,outfile,extra,depth+1)
        return {"records_written":left["records_written"]+right["records_written"],
                "warnings":left["warnings"]+right["warnings"],
                "calls":1+left["calls"]+right["calls"]}

    # Safe — write directly to disk
    for rec in result["records"]:
        outfile.write(json.dumps(rec)+"\n")
    outfile.flush()

    state["total_records"] = state.get("total_records",0) + result["count"]
    if result["count"] > 0:
        state["avg_bytes_observed"] = round(result["result_bytes"]/result["count"], 1)
    write_job_state(jid, state)
    append_job_log(jid, {"type":"window_done","from":fmt_ms(ws),"to":fmt_ms(we),
                          "depth":depth,"count":result["count"]})
    return {"records_written":result["count"],"warnings":[],"calls":1}

# ── Resume ────────────────────────────────────────────────────────────────────
def find_resume_idx(jid: str, schedule: list) -> int:
    completed = set()
    lp = job_log_path(jid)
    if not os.path.exists(lp): return 0
    with open(lp) as f:
        for line in f:
            try:
                ev = json.loads(line)
                if ev.get("type") == "window_done":
                    completed.add((ev["from"], ev["to"]))
            except: pass
    for i, w in enumerate(schedule):
        if (fmt_ms(w["start"]), fmt_ms(w["end"])) not in completed:
            return i
    return len(schedule)

# ── Main Job ──────────────────────────────────────────────────────────────────
def run_job(jid: str, dql: str, start_dt: datetime, end_dt: datetime,
            outpath: str, extra: list, workers: int = 1):
    t0 = time.time()
    try:
        # Phase 1: preflight
        state = read_job_state(jid)
        state["status"] = "preflight"
        write_job_state(jid, state)
        pf = run_preflight(jid, dql, start_dt, end_dt)
        avg_b = pf.get("avg_bytes", 500)

        state = read_job_state(jid)
        state.update({"avg_bytes":round(avg_b,1),
                      "total_expected":pf.get("total_expected",0),
                      "preflight_done":True,
                      "workers":workers})
        write_job_state(jid, state)

        # Phase 2: build schedule
        schedule     = build_schedule(pf, start_dt, end_dt)
        total_windows = len(schedule)
        state = read_job_state(jid)
        state["total_windows"] = total_windows
        write_job_state(jid, state)
        append_job_log(jid, {"type":"schedule_built","windows":total_windows,
                              "avg_bytes":round(avg_b,1)})

        # Phase 3: find resume point
        resume_idx = 0
        resumed    = False
        if os.path.exists(outpath) and os.path.getsize(outpath) > 0:
            resume_idx = find_resume_idx(jid, schedule)
            resumed    = resume_idx > 0
            if resumed:
                append_job_log(jid, {"type":"resumed","from_window":resume_idx,
                                      "total":total_windows})

        state = read_job_state(jid)
        state.update({"status":"running","resumed":resumed})
        write_job_state(jid, state)

        # Phase 4: extraction loop
        mode = "a" if resumed else "w"
        window_durations = []  # rolling observed window times for ETA

        def _check_interrupt() -> bool:
            """Return True if job should stop — writes state accordingly."""
            if should_stop(jid):
                st = read_job_state(jid)
                st.update({"status":"stopped","stopped_at":now_iso(),"resume_available":True})
                write_job_state(jid, st)
                append_job_log(jid, {"type":"stopped",
                                      "window_idx":st.get("windows_done",0),
                                      "records":st.get("total_records",0)})
                _partial_summary(jid, t0, outpath, pf, total_windows, 0)
                return True
            if should_pause(jid):
                st = read_job_state(jid)
                st.update({"status":"paused","paused_at":now_iso(),
                           "resume_available":True,"pause_requested":False})
                write_job_state(jid, st)
                append_job_log(jid, {"type":"paused"})
                _partial_summary(jid, t0, outpath, pf, total_windows, 0)
                return True
            return False

        def _run_one_window(idx: int, w: dict, tmp_path: str, obs_avg: float) -> dict:
            """
            Run a single window, write to tmp_path.
            Returns result dict with timing info.
            """
            append_job_log(jid, {"type":"window_start",
                                  "window":idx+1,"total":total_windows,
                                  "from":fmt_ms(w["start"]),"to":fmt_ms(w["end"]),
                                  "expected":w["expected"],
                                  "max_records":w["max_records"],
                                  "max_bytes_mb":round(w["max_bytes"]/1024/1024,1)})
            ts = time.time()
            with open(tmp_path, "w") as tf:
                res = extract_recursive(jid, dql, w["start"], w["end"],
                                        w["max_records"], w["max_bytes"],
                                        obs_avg, tf, extra, depth=0)
            dur = time.time() - ts
            return {"idx":idx, "res":res, "dur":dur, "tmp":tmp_path}

        with open(outpath, mode) as outfile:
            idx = resume_idx
            while idx < total_windows:
                # Check interrupt before each batch
                if _check_interrupt(): return

                # Schedule window check
                state = read_job_state(jid)
                first_sched_log = True
                while not is_within_run_window(state):
                    if first_sched_log:
                        nxt = _next_window(state.get("schedule",{}))
                        state["status"]     = "scheduled_pause"
                        state["resumes_at"] = nxt
                        write_job_state(jid, state)
                        append_job_log(jid, {"type":"scheduled_pause","resumes_at":nxt})
                        first_sched_log = False
                    time.sleep(60)
                    state = read_job_state(jid)
                    if should_stop(jid) or should_pause(jid): break
                if _check_interrupt(): return

                # Build batch of `workers` windows
                batch = []
                for b in range(workers):
                    if idx + b < total_windows:
                        w = schedule[idx + b]
                        tmp = os.path.join(JOBS_DIR, f"{jid}.win{idx+b}.tmp")
                        batch.append((idx + b, w, tmp))

                # Update state — show first window of batch as current
                state = read_job_state(jid)
                obs_avg = state.get("avg_bytes_observed", avg_b)
                state.update({"status":"running",
                              "current_window_from":fmt_ms(batch[0][1]["start"]),
                              "current_window_to":  fmt_ms(batch[-1][1]["end"]),
                              "windows_done":idx,
                              "progress_pct":round((idx/total_windows)*100,1)})
                write_job_state(jid, state)

                if workers == 1:
                    # Sequential — simple path, no temp files needed
                    bi, w, tmp = batch[0]
                    res_info = _run_one_window(bi, w, tmp, obs_avg)
                    results = [res_info]
                else:
                    # Parallel — run batch concurrently, each to own temp file
                    results = []
                    with ThreadPoolExecutor(max_workers=workers) as ex:
                        futs = {ex.submit(_run_one_window, bi, w, tmp, obs_avg): bi
                                for bi, w, tmp in batch}
                        for fut in as_completed(futs):
                            results.append(fut.result())
                    # Sort by window index to merge in correct order
                    results.sort(key=lambda x: x["idx"])

                # Check interrupt after batch — before merging
                interrupted = should_stop(jid) or should_pause(jid)

                # Merge temp files to output in order
                for r in results:
                    tmp = r["tmp"]
                    if os.path.exists(tmp):
                        if not interrupted or r["res"]["records_written"] > 0:
                            with open(tmp) as tf:
                                for line in tf:
                                    outfile.write(line)
                            outfile.flush()
                        os.remove(tmp)

                if interrupted:
                    _check_interrupt()
                    return

                # Update stats after batch
                batch_dur = sum(r["dur"] for r in results) / len(results)  # avg
                window_durations.append(batch_dur)
                if len(window_durations) > 20: window_durations.pop(0)
                avg_window_secs = sum(window_durations) / len(window_durations)
                idx += len(batch)
                remain  = total_windows - idx
                eta_min = round(avg_window_secs * remain / 60, 1)

                for r in results:
                    bi = r["idx"]
                    res = r["res"]
                    dur = r["dur"]
                    state2 = read_job_state(jid)
                    state2["windows_done"]   = bi + 1
                    state2["progress_pct"]   = round(((bi+1)/total_windows)*100, 1)
                    state2["eta_minutes"]    = eta_min
                    state2["avg_window_sec"] = round(avg_window_secs, 1)
                    write_job_state(jid, state2)

                    append_job_log(jid, {"type":"window_complete",
                                          "window":bi+1,
                                          "records":res["records_written"],
                                          "calls":res.get("calls",0),
                                          "splits":max(0,res.get("calls",0)-1),
                                          "warnings":len(res.get("warnings",[])),
                                          "duration_sec":round(dur,1),
                                          "eta_minutes":eta_min})

                state = read_job_state(jid)
                sz    = os.path.getsize(outpath) if os.path.exists(outpath) else 0
                append_job_log(jid, {"type":"progress",
                                      "pct":round(idx/total_windows*100,1),
                                      "records":state.get("total_records",0),
                                      "mb":round(sz/1024/1024,1),
                                      "eta_minutes":eta_min,
                                      "avg_window_sec":round(avg_window_secs,1),
                                      "windows_done":idx,
                                      "windows_total":total_windows})

        # Phase 5: final summary
        _final_summary(jid, t0, outpath, pf, dql, start_dt, end_dt, total_windows)

    except Exception as e:
        state = read_job_state(jid)
        state.update({"status":"error","error":str(e)})
        write_job_state(jid, state)
        append_job_log(jid, {"type":"job_error","error":str(e)})

def _final_summary(jid, t0, outpath, pf, dql, start_dt, end_dt, total_windows):
    elapsed = time.time() - t0
    state   = read_job_state(jid)
    sz      = os.path.getsize(outpath) if os.path.exists(outpath) else 0
    actual  = state.get("total_records", 0)
    expect  = pf.get("total_expected", 0)
    delta   = expect - actual
    dpct    = round(abs(delta)/expect*100,3) if expect > 0 else 0
    splits  = state.get("api_calls",0) - total_windows

    summary = {
        "job_id": jid,
        "status": "done",
        "input":  {"dql":dql,"from":fmt_ms(start_dt),"to":fmt_ms(end_dt),
                   "range_days":round((end_dt-start_dt).total_seconds()/86400,1)},
        "preflight": {"avg_bytes_per_record":round(pf.get("avg_bytes",0),1),
                      "interval":pf.get("interval",""),
                      "total_expected":expect,
                      "skipped":pf.get("skipped",False)},
        "extraction": {"total_records":actual,"total_mb":round(sz/1024/1024,2),
                       "total_windows":total_windows,"api_calls":state.get("api_calls",0),
                       "splits_triggered":max(0,splits),
                       "errors":state.get("errors",0),"warnings":state.get("warnings",0),
                       "duration_minutes":round(elapsed/60,2)},
        "integrity": {"expected":expect,"actual":actual,"delta":delta,
                      "delta_pct":dpct,"complete":dpct < 1.0},
        "output":    {"filename":os.path.basename(outpath),
                      "size_mb":round(sz/1024/1024,2),
                      "resumed":state.get("resumed",False)},
        "completed_at": now_iso()
    }
    write_job_summary(jid, summary)
    state.update({"status":"done","progress_pct":100.0,
                  "completed_at":now_iso(),"summary":summary})
    write_job_state(jid, state)
    append_job_log(jid, {"type":"job_done","total_records":actual,
                          "delta_pct":dpct,"duration_minutes":round(elapsed/60,2)})

def _partial_summary(jid, t0, outpath, pf, total_windows, windows_done):
    elapsed = time.time() - t0
    state   = read_job_state(jid)
    sz      = os.path.getsize(outpath) if os.path.exists(outpath) else 0
    summary = {"job_id":jid,"status":state.get("status","partial"),"partial":True,
               "extraction":{"total_records":state.get("total_records",0),
                              "total_mb":round(sz/1024/1024,2),
                              "windows_done":windows_done,"windows_total":total_windows,
                              "api_calls":state.get("api_calls",0),
                              "errors":state.get("errors",0),
                              "warnings":state.get("warnings",0),
                              "duration_minutes":round(elapsed/60,2)},
               "resume_available":True,"saved_at":now_iso()}
    write_job_summary(jid, summary)

def _next_window(schedule: dict) -> str:
    try:
        import pytz
        tz       = pytz.timezone(schedule.get("timezone","UTC"))
        loc      = datetime.now(tz)
        run_from = schedule.get("run_from_hour", 22)
        if loc.hour < run_from:
            nxt = loc.replace(hour=run_from, minute=0, second=0, microsecond=0)
        else:
            nxt = (loc + timedelta(days=1)).replace(
                hour=run_from, minute=0, second=0, microsecond=0)
        return nxt.isoformat()
    except: return ""

# ── Extract API ───────────────────────────────────────────────────────────────
class ExtractRequest(BaseModel):
    dql_template:    str
    start:           str
    end:             str
    output_filename: str = ""
    extra_args:      list = []
    schedule:        Optional[dict] = None
    workers:         int = 1   # parallel workers: 1=sequential, 2=2x speed

@app.post("/api/extract/start")
def extract_start(req: ExtractRequest):
    if not req.output_filename: raise HTTPException(400,"output_filename required")
    safe    = re.sub(r"[^a-zA-Z0-9._-]","_",req.output_filename)
    outpath = os.path.join(OUTDIR, safe)
    start_dt = datetime.fromisoformat(req.start.replace("Z","+00:00"))
    end_dt   = datetime.fromisoformat(req.end.replace("Z","+00:00"))
    jid = str(uuid.uuid4())[:8]
    workers = max(1, min(3, req.workers or 1))  # clamp 1-3 — must be before write_job_state
    write_job_state(jid, {"status":"starting","job_id":jid,"filename":safe,"workers":workers,
                           "outpath":outpath,"dql":req.dql_template,
                           "start":fmt_ms(start_dt),"end":fmt_ms(end_dt),
                           "schedule":req.schedule,"total_records":0,
                           "api_calls":0,"warnings":0,"errors":0,
                           "started_at":now_iso(),"resume_available":False})
    def _safe_run_job():
        try:
            run_job(jid,req.dql_template,start_dt,end_dt,outpath,req.extra_args,workers)
        except Exception as e:
            import traceback
            print(f"[FATAL] Job {jid} thread crashed: {e}")
            print(traceback.format_exc())
            try:
                st = read_job_state(jid)
                st.update({"status":"error","error":str(e),"crashed_at":now_iso()})
                write_job_state(jid, st)
                append_job_log(jid, {"type":"job_error","error":str(e),
                                      "traceback":traceback.format_exc()[-500:]})
            except Exception: pass

    threading.Thread(target=_safe_run_job, daemon=False).start()
    return {"job_id":jid,"filename":safe,"status":"started","workers":workers}

@app.post("/api/extract/job/{job_id}/pause")
def pause_job(job_id: str):
    jid   = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    state = read_job_state(jid)
    if not state: raise HTTPException(404,"Job not found")
    if state.get("status") not in ("running","scheduled_pause"):
        raise HTTPException(400,f"Cannot pause: {state.get('status')}")
    state["pause_requested"] = True
    write_job_state(jid, state)
    return {"job_id":jid,"action":"pause_requested"}

@app.post("/api/extract/job/{job_id}/stop")
def stop_job(job_id: str):
    jid   = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    state = read_job_state(jid)
    if not state: raise HTTPException(404,"Job not found")
    state["stop_requested"] = True
    write_job_state(jid, state)
    return {"job_id":jid,"action":"stop_requested"}

@app.post("/api/extract/job/{job_id}/resume")
def resume_job(job_id: str):
    jid   = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    state = read_job_state(jid)
    if not state: raise HTTPException(404,"Job not found")
    if state.get("status") not in ("paused","stopped","interrupted","scheduled_pause"):
        raise HTTPException(400,f"Cannot resume: {state.get('status')}")
    state.update({"pause_requested":False,"stop_requested":False,"status":"starting"})
    write_job_state(jid, state)
    start_dt = datetime.fromisoformat(state["start"].replace("Z","+00:00"))
    end_dt   = datetime.fromisoformat(state["end"].replace("Z","+00:00"))
    _rjid = jid
    _rdql = state["dql"]
    _rs   = start_dt
    _re   = end_dt
    _rout = state["outpath"]
    _rwrk = state.get("workers", 1)

    def _safe_resume():
        try:
            run_job(_rjid, _rdql, _rs, _re, _rout, [], _rwrk)
        except Exception as e:
            import traceback
            print(f"[FATAL] Job {_rjid} resume crashed: {e}")
            try:
                st = read_job_state(_rjid)
                st.update({"status":"error","error":str(e),"crashed_at":now_iso()})
                write_job_state(_rjid, st)
                append_job_log(_rjid, {"type":"job_error","error":str(e),
                                        "traceback":traceback.format_exc()[-500:]})
            except Exception: pass

    threading.Thread(target=_safe_resume, daemon=False).start()
    return {"job_id":jid,"action":"resumed"}

@app.get("/api/extract/job/{job_id}")
def get_job(job_id: str):
    jid   = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    state = read_job_state(jid)
    if not state: raise HTTPException(404,"Job not found")
    op = state.get("outpath","")
    if op and os.path.exists(op):
        state["size_mb"] = round(os.path.getsize(op)/1024/1024,2)
    return state

@app.get("/api/extract/job/{job_id}/log")
def get_job_log(job_id: str, tail: int = 100):
    jid = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    lp  = job_log_path(jid)
    if not os.path.exists(lp): raise HTTPException(404,"Log not found")
    r = subprocess.run(["tail","-n",str(tail),lp],capture_output=True,text=True)
    evs = []
    for line in r.stdout.strip().split("\n"):
        if line:
            try: evs.append(json.loads(line))
            except: pass
    return {"job_id":jid,"events":evs}

@app.get("/api/extract/job/{job_id}/summary")
def get_job_summary(job_id: str):
    jid = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    sp  = job_summary_path(jid)
    if not os.path.exists(sp): raise HTTPException(404,"Summary not found")
    with open(sp) as f: return json.load(f)

@app.get("/api/extract/jobs")
def list_jobs():
    jobs = []
    for f in os.listdir(JOBS_DIR):
        if not f.endswith(".state.json"): continue
        jid   = f.replace(".state.json","")
        state = read_job_state(jid)
        op    = state.get("outpath","")
        if op and os.path.exists(op):
            state["size_mb"] = round(os.path.getsize(op)/1024/1024,2)
        state["has_summary"] = os.path.exists(job_summary_path(jid))
        jobs.append(state)
    return {"jobs":sorted(jobs,key=lambda x:x.get("started_at",""),reverse=True)}

@app.delete("/api/extract/job/{job_id}")
def delete_job(job_id: str):
    jid = re.sub(r"[^a-zA-Z0-9-]","",job_id)
    for ext in (".state.json",".log.jsonl",".summary.json"):
        p = os.path.join(JOBS_DIR, jid+ext)
        if os.path.exists(p): os.remove(p)
    return {"deleted":jid}

# ── Health & Config ───────────────────────────────────────────────────────────
class ConfigReq(BaseModel):
    environment:  str
    token:        str
    context_name: str = "default"

class CmdReq(BaseModel):
    args:            list
    write_to_disk:   bool = False
    output_filename: str  = ""

class ExportReq(BaseModel):
    data: str
    mode: str = "full"
    fmt:  str = "json"

@app.get("/api/health")
def health():
    r = run_dtctl(["--version"])
    return {"status":"ok","dtctl":r["stdout"].strip()}

@app.post("/api/config")
def set_config(req: ConfigReq):
    r1 = run_dtctl(["config","set-credentials",req.context_name,"--token",req.token])
    if not r1["success"]: raise HTTPException(400,r1["stderr"])
    r2 = run_dtctl(["config","set-context",req.context_name,
                    "--environment",req.environment,"--token-ref",req.context_name])
    if not r2["success"]: raise HTTPException(400,r2["stderr"])
    run_dtctl(["ctx",req.context_name])
    return {"success":True}

@app.post("/api/command/stream")
def stream(req: CmdReq):
    outpath = None
    args    = list(req.args)
    if req.write_to_disk and req.output_filename:
        safe    = re.sub(r"[^a-zA-Z0-9._-]","_",req.output_filename)
        outpath = os.path.join(OUTDIR,safe)
        args    = [a for a in args if a not in ('-v','-vv','--debug')]
        if '-o' not in args and '--output' not in args:
            args.extend(['-o','json'])

    def generate():
        outfile = None
        try:
            if outpath:
                outfile = open(outpath,"w")
                proc = subprocess.Popen([DTCTL]+args,stdout=outfile,
                                        stderr=subprocess.PIPE,text=True,env={**os.environ})
            else:
                proc = subprocess.Popen([DTCTL]+args,stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,text=True,env={**os.environ})
            q2 = queue.Queue()
            stdout_lines = []
            def rd_err():
                for l in proc.stderr: q2.put(("err",l.rstrip()))
                q2.put(("done_err",None))
            def rd_out():
                if outpath: q2.put(("done_out",None)); return
                for l in proc.stdout: q2.put(("out",l.rstrip()))
                q2.put(("done_out",None))
            threading.Thread(target=rd_err,daemon=True).start()
            threading.Thread(target=rd_out,daemon=True).start()
            done=0; last_sz=0
            while done < 2:
                try: kind,line = q2.get(timeout=0.5)
                except queue.Empty:
                    if outpath and os.path.exists(outpath):
                        sz = os.path.getsize(outpath)
                        if sz != last_sz:
                            last_sz=sz
                            yield f"data: {json.dumps({'type':'disk_progress','mb':round(sz/1024/1024,2)})}\n\n"
                    yield f"data: {json.dumps({'type':'ping'})}\n\n"
                    continue
                if kind in ("done_err","done_out"): done+=1; continue
                if kind=="out": stdout_lines.append(line)
                elif kind=="err":
                    c = clean_debug(line)
                    if c: yield f"data: {json.dumps({'type':'debug','line':c})}\n\n"
            proc.wait()
            if outfile: outfile.close()
            fi = None
            if outpath and os.path.exists(outpath):
                sz = os.path.getsize(outpath)
                wc = subprocess.run(["wc","-l",outpath],capture_output=True,text=True)
                lc = int(wc.stdout.split()[0]) if wc.returncode==0 else 0
                fi = {"filename":os.path.basename(outpath),
                      "size_mb":round(sz/1024/1024,2),"lines":lc}
            yield f"data: {json.dumps({'type':'done','returncode':proc.returncode,'success':proc.returncode==0,'stdout':chr(10).join(stdout_lines) if not outpath else '','file':fi})}\n\n"
        except Exception as e:
            if outfile:
                try: outfile.close()
                except: pass
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"
    return StreamingResponse(generate(),media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

# ── Files ─────────────────────────────────────────────────────────────────────
@app.get("/api/files")
def list_files():
    files = []
    # Index job filenames for O(1) lookup
    job_map = {}
    for jf in os.listdir(JOBS_DIR):
        if not jf.endswith(".state.json"): continue
        jid = jf.replace(".state.json","")
        st  = read_job_state(jid)
        fn  = st.get("filename","")
        if fn: job_map[fn] = {"job_id":jid,"status":st.get("status",""),
                               "has_summary":os.path.exists(job_summary_path(jid))}
    for f in os.listdir(OUTDIR):
        fp  = os.path.join(OUTDIR,f)
        s   = os.stat(fp)
        job = job_map.get(f,{})
        files.append({"filename":f,"size_mb":round(s.st_size/1024/1024,2),
                      "modified":s.st_mtime,**job})
    return {"files":sorted(files,key=lambda x:x["modified"],reverse=True)}

@app.get("/api/files/{filename}/preview")
def preview(filename: str, lines: int = 20, position: str = "tail"):
    safe = re.sub(r"[^a-zA-Z0-9._-]","_",filename)
    fp   = os.path.join(OUTDIR,safe)
    if not os.path.exists(fp): raise HTTPException(404,"Not found")
    cmd = ["head","-n",str(lines),fp] if position=="head" else ["tail","-n",str(lines),fp]
    r   = subprocess.run(cmd,capture_output=True,text=True)
    wc  = subprocess.run(["wc","-l",fp],capture_output=True,text=True)
    tot = int(wc.stdout.split()[0]) if wc.returncode==0 else 0
    return {"filename":filename,"preview":r.stdout,"total_lines":tot,
            "size_mb":round(os.path.getsize(fp)/1024/1024,2),"position":position}

@app.get("/api/files/{filename}/summary")
def file_summary(filename: str):
    safe = re.sub(r"[^a-zA-Z0-9._-]","_",filename)
    for jf in os.listdir(JOBS_DIR):
        if not jf.endswith(".state.json"): continue
        jid = jf.replace(".state.json","")
        if read_job_state(jid).get("filename") == safe:
            sp = job_summary_path(jid)
            if os.path.exists(sp):
                with open(sp) as f: return json.load(f)
    raise HTTPException(404,"No summary for this file")

@app.get("/api/files/{filename}/download")
def download(filename: str):
    safe = re.sub(r"[^a-zA-Z0-9._-]","_",filename)
    fp   = os.path.join(OUTDIR,safe)
    if not os.path.exists(fp): raise HTTPException(404,"Not found")
    return FileResponse(fp,filename=filename,media_type="application/octet-stream")

@app.delete("/api/files/{filename}")
def delete_file(filename: str):
    safe = re.sub(r"[^a-zA-Z0-9._-]","_",filename)
    fp   = os.path.join(OUTDIR,safe)
    if not os.path.exists(fp): raise HTTPException(404,"Not found")
    os.remove(fp)
    return {"success":True}

@app.post("/api/export/full")
def exp_full(req: ExportReq):
    try:
        if req.fmt=="json":
            p    = json.loads(req.data)
            recs = p.get("records") or []
        else: recs = list(csv.DictReader(io.StringIO(req.data)))
        if not recs: raise HTTPException(400,"No records")
        flat = [{k:(json.dumps(v) if isinstance(v,(dict,list)) else v)
                  for k,v in r.items()} for r in recs]
        keys = list(dict.fromkeys(k for r in flat for k in r))
        buf  = io.StringIO()
        w    = csv.DictWriter(buf,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(flat)
        return Response(buf.getvalue(),media_type="text/csv",
                        headers={"Content-Disposition":"attachment; filename=export.csv"})
    except HTTPException: raise
    except Exception as e: raise HTTPException(400,str(e))

@app.post("/api/export/content")
def exp_content(req: ExportReq):
    try:
        lines=[]
        if req.fmt=="json":
            p=json.loads(req.data)
            for r in (p.get("records") or []):
                lines.append(str(r.get("content") or r.get("log.content") or ""))
        else:
            for row in csv.DictReader(io.StringIO(req.data)):
                lines.append(str(row.get("content") or ""))
        return Response("\n".join(lines),media_type="text/plain",
                        headers={"Content-Disposition":"attachment; filename=content.log"})
    except Exception as e: raise HTTPException(400,str(e))

# ── Resource Endpoints ────────────────────────────────────────────────────────
@app.get("/api/workflows")
def workflows():
    r=run_dtctl(["get","workflows","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

@app.get("/api/dashboards")
def dashboards():
    r=run_dtctl(["get","dashboards","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

@app.get("/api/buckets")
def buckets():
    r=run_dtctl(["get","buckets","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

@app.get("/api/slos")
def slos():
    r=run_dtctl(["get","slos","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

# ── Ingest ────────────────────────────────────────────────────────────────────
INGEST_CONFIG_FILE = "/root/.config/dtctl/ingest_config.json"

class IngestPayload(BaseModel):
    events: list

class IngestConfig(BaseModel):
    ingest_url:   str
    ingest_token: str

@app.post("/api/ingest/send")
def ingest_send(payload: IngestPayload):
    import requests as req
    try:
        if not os.path.exists(INGEST_CONFIG_FILE): raise HTTPException(400,"Ingest not configured")
        with open(INGEST_CONFIG_FILE) as f: cfg=json.load(f)
        url=cfg.get("ingest_url","").rstrip("/"); token=cfg.get("ingest_token","")
        if not url or not token: raise HTTPException(400,"Ingest URL or token missing")
        url=re.sub(r'https://([^.]+)\.apps\.dynatrace\.com',
                   r'https://\1.live.dynatrace.com',url).rstrip("/")
        if not url.endswith("/api/v2/logs/ingest"): url+="/api/v2/logs/ingest"
        resp=req.post(url,headers={"Authorization":"Api-Token "+token,
                                    "Content-Type":"application/json; charset=utf-8"},
                      json=payload.events,timeout=30)
        return {"status":resp.status_code,"ok":resp.status_code in (200,202,204),"body":resp.text[:500]}
    except req.exceptions.RequestException as e: raise HTTPException(502,str(e))

@app.post("/api/ingest-config")
def save_ingest_config(cfg: IngestConfig):
    try:
        os.makedirs(os.path.dirname(INGEST_CONFIG_FILE),exist_ok=True)
        with open(INGEST_CONFIG_FILE,"w") as f:
            json.dump({"ingest_url":cfg.ingest_url,"ingest_token":cfg.ingest_token},f)
        return {"success":True}
    except Exception as e: raise HTTPException(500,str(e))

@app.get("/api/ingest-config")
def get_ingest_config():
    try:
        if os.path.exists(INGEST_CONFIG_FILE):
            with open(INGEST_CONFIG_FILE) as f: d=json.load(f)
            return {"success":True,"ingest_url":d.get("ingest_url",""),
                    "ingest_token":d.get("ingest_token","")}
        return {"success":True,"ingest_url":"","ingest_token":""}
    except: return {"success":False,"ingest_url":"","ingest_token":""}

# ── Static ────────────────────────────────────────────────────────────────────
app.mount("/static",StaticFiles(directory="/app/frontend"),name="static")

@app.get("/")
def root(): return FileResponse("/app/frontend/index.html")
