"""
extractor.py — Grailkeeper v3.0
Extraction engine: recursive binary window splitting driven by API notifications.

Strategy:
  1. Start with configurable initial window (e.g. full range / 24h / 1h)
  2. Query each window with max_records cap
  3. If response has ANY notification → window truncated → split in half → retry
  4. If no notification AND count < max_records → window clean → write to disk
  5. Recurse until max_depth reached (then write anyway with warning)

No summarize preflight. No dtctl. Pure Python.
"""

import json
import csv
import os
import threading
import logging
import time

from datetime import datetime, timezone, timedelta
from typing    import Optional, Callable

import grail_engine as ge

log = logging.getLogger("extractor")

# ── Defaults (overridden by Settings) ─────────────────────────────────────────
DEFAULT_MAX_RECORDS   = 500_000
DEFAULT_MAX_DEPTH     = 10
DEFAULT_INITIAL_HOURS = 24      # hours per initial window (0 = full range)


# ── Job state ──────────────────────────────────────────────────────────────────
class ExtractionJob:
    def __init__(self, jid: str, dql: str, start: datetime, end: datetime,
                 outpath: str, fmt: str,
                 max_records: int, max_depth: int, initial_hours: int,
                 append_log: Callable):
        self.jid           = jid
        self.dql           = dql
        self.start         = start
        self.end           = end
        self.outpath       = outpath
        self.fmt           = fmt            # "jsonl", "csv", or "raw"
        self.fields        = []            # for raw format
        self.max_records   = max_records
        self.max_depth     = max_depth
        self.initial_hours = initial_hours  # 0 = full range
        self.append_log    = append_log

        self.probe         = False      # set by caller if profiling enabled
        self.stop_flag     = threading.Event()
        self.pause_flag    = threading.Event()  # set to pause, clear to resume
        # Attach a mutable dict for Grail query cancellation
        self.stop_flag._grail_token_ref  = {}
        self.pause_flag._grail_token_ref = {}
        self.lock          = threading.Lock()

        # Stats
        self.total_records    = 0
        self.total_api_calls  = 0
        self.total_splits     = 0
        self.total_scanned_b  = 0
        self.total_scanned_r  = 0
        self.windows_total    = 0
        self.windows_done     = 0
        self.windows_current  = 0
        self.errors           = 0
        self.warnings         = 0
        self.started_at       = datetime.now(timezone.utc)

        # CSV writer state
        self._csv_file        = None
        self._csv_writer      = None
        self._csv_headers     = None

    def log(self, msg: str, level: str = "info"):
        self.append_log(self.jid, {
            "type": "log",
            "level": level,
            "msg": msg,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def stopped(self) -> bool:
        return self.stop_flag.is_set()

    def cancel_current_query(self):
        """Cancel any in-flight Grail query immediately via requestToken."""
        import requests as _r
        for flag in [self.stop_flag, self.pause_flag]:
            ref = getattr(flag, '_grail_token_ref', {})
            token = ref.get('token')
            session = ref.get('session')
            base_url = ref.get('base_url')
            if token and base_url:
                try:
                    s = session or _r.Session()
                    import grail_engine as _ge
                    s.headers.update(_ge._headers())
                    s.post(base_url + "/query:cancel",
                           params={"request-token": token}, timeout=5)
                    ref.clear()
                    log.info(f"[cancel] Cancelled Grail query {token[:16]}...")
                except Exception as e:
                    log.warning(f"[cancel] Cancel failed: {e}")


# ── Writer helpers ─────────────────────────────────────────────────────────────
def _write_records(job: ExtractionJob, records: list, records_jsonl: str = None):
    """Write records to output. If records_jsonl is provided, stream directly
    from that JSONL temp file to avoid loading all records into RAM."""
    if records_jsonl:
        _stream_from_jsonl(job, records_jsonl)
        return
    if not records:
        return
    if job.fmt == "csv":
        _write_csv(job, records)
    elif job.fmt == "raw":
        _write_raw(job, records)
    else:
        _write_jsonl(job, records)
    with job.lock:
        job.total_records += len(records)


def _stream_from_jsonl(job: ExtractionJob, jsonl_path: str):
    """Stream records from a JSONL temp file directly to the output file.
    Never loads all records into RAM simultaneously."""
    import os
    count = 0
    try:
        if job.fmt == "jsonl":
            # Direct file copy — most efficient
            with open(jsonl_path, 'r', encoding='utf-8') as src,                  open(job.outpath, 'a', encoding='utf-8') as dst:
                for line in src:
                    if line.strip():
                        dst.write(line)
                        count += 1
        else:
            # Parse line by line for CSV/raw formats
            with open(jsonl_path, 'r', encoding='utf-8') as src:
                for line in src:
                    line = line.strip()
                    if line:
                        rec = json.loads(line)
                        if job.fmt == "csv":
                            _write_csv(job, [rec])
                        else:
                            _write_raw(job, [rec])
                        count += 1
    finally:
        if os.path.exists(jsonl_path):
            os.unlink(jsonl_path)
    with job.lock:
        job.total_records += count


def _write_raw(job: ExtractionJob, records: list):
    """
    Write records in raw format.
    Always prepends timestamp field first, then a dash separator, then the content fields.
    Format: <timestamp> - <field_value>
    If multiple content fields: <timestamp> - <field1> | <field2> | ...
    """
    fields = job.fields if job.fields else ["content"]
    with open(job.outpath, "a", encoding="utf-8") as f:
        for rec in records:
            # Always include timestamp prefix
            ts = rec.get("timestamp", rec.get("@timestamp", ""))
            # Get the requested fields values
            if len(fields) == 1:
                val = rec.get(fields[0], "")
                line = f"{ts} | {val}" if ts else str(val)
            else:
                parts = [str(rec.get(field, "")) for field in fields if field in rec]
                val_str = " | ".join(parts)
                line = f"{ts} | {val_str}" if ts else val_str
            f.write(line + "\n")


def _write_jsonl(job: ExtractionJob, records: list):
    with open(job.outpath, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _write_csv(job: ExtractionJob, records: list):
    if not records:
        return
    mode    = "a" if os.path.exists(job.outpath) else "w"
    headers = list(records[0].keys())

    if job._csv_headers is None:
        job._csv_headers = headers

    with open(job.outpath, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=job._csv_headers,
                                extrasaction="ignore")
        if mode == "w":
            writer.writeheader()
        for rec in records:
            writer.writerow(rec)


# ── Format helpers ─────────────────────────────────────────────────────────────
def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def _human(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ── Core recursive splitter ────────────────────────────────────────────────────
def _extract_window(job: ExtractionJob,
                    ws: datetime, we: datetime,
                    depth: int = 0,
                    retry: int = 0):
    """
    Recursively extract [ws, we].
    - Query the window
    - If clean (no notifications, count < max_records) → write to disk
    - If truncated (any notification OR count == max_records) → split in half
    - If depth >= max_depth → write anyway with warning
    """
    if job.stopped():
        return

    duration_s = (we - ws).total_seconds()

    job.log(f"[depth={depth}] {_human(ws)} → {_human(we)} "
            f"({duration_s/3600:.2f}h)")

    # ── Pause check ───────────────────────────────────────────────────────────
    if job.pause_flag.is_set() and not job.stopped():
        job.log("⏸ Paused — click Resume to continue")
    while job.pause_flag.is_set() and not job.stopped():
        time.sleep(0.2)

    # ── Query ──────────────────────────────────────────────────────────────────
    try:
        result = ge.query(
            dql         = job.dql,
            start       = _fmt(ws),
            end         = _fmt(we),
            max_records = job.max_records,
            strip_limit = True,
            stop_flag   = job.stop_flag,
        )
    except InterruptedError:
        job.log("⏹ Stopped by user", "warn")
        return
    except Exception as e:
        job.log(f"❌ API error on window [{ws.strftime('%Y-%m-%d %H:%M')}→{we.strftime('%Y-%m-%d %H:%M')}]: {e}", "error")
        with job.lock:
            job.errors += 1
        # Retry up to 2 more times before giving up
        if retry < 2:
            wait = (retry + 1) * 5  # 5s, 10s
            job.log(f"⟳ Retrying window in {wait}s (attempt {retry+2}/3)...", "warn")
            time.sleep(wait)
            _extract_window(job, ws, we, depth, retry=retry+1)
        else:
            job.log(f"❌ Window [{fmt(ws)}→{fmt(we)}] failed after 3 attempts — data may be missing", "error")
        return

    with job.lock:
        job.total_api_calls  += 1
        job.total_scanned_b  += result.get("scanned_bytes", 0)
        job.total_scanned_r  += result.get("scanned_records", 0)

    count     = result["count"]
    truncated = result["truncated"]
    reasons   = result["trunc_reasons"]
    exec_ms   = result["execution_ms"]

    job.log(
        f"  → {count:,} records | "
        f"scanned {result['scanned_bytes']//1024//1024:,}MB "
        f"/ {result['scanned_records']:,} recs | "
        f"{exec_ms}ms | "
        f"truncated={truncated}"
        + (f" reasons={reasons}" if reasons else "")
    )

    # ── Truncated → split ──────────────────────────────────────────────────────
    if truncated:
        if depth >= job.max_depth:
            # Max depth reached — write what we have with a warning
            job.log(
                f"  ⚠️  Max depth {job.max_depth} reached — writing {count:,} "
                f"records (may be incomplete)", "warn"
            )
            with job.lock:
                job.warnings += 1
            _write_records(job, result["records"], result.get("records_jsonl"))
            return

        with job.lock:
            job.total_splits += 1

        mid = ws + (we - ws) / 2
        job.log(f"  ↕ Split → [{_human(ws)}→{_human(mid)}] "
                f"[{_human(mid)}→{_human(we)}]")

        _extract_window(job, ws,  mid, depth + 1)
        if not job.stopped():
            _extract_window(job, mid, we,  depth + 1)
        return

    # ── Clean → write ──────────────────────────────────────────────────────────
    if count > 0:
        _write_records(job, result["records"], result.get("records_jsonl"))
        job.log(f"  ✅ Written {count:,} records")
    else:
        job.log(f"  ○ Empty window")



# ── Pre-flight binary search profiler ─────────────────────────────────────────
def probe_has_data(dql: str, start: datetime, end: datetime,
                   stop_flag=None) -> bool:
    """
    Check if any records exist in [start, end] using | limit 1.
    Cost: ~10-80MB scan, ~10-50ms. Returns True if ≥1 record found.
    """
    import re as _re
    # Strip limit and inject timeframe, then add | limit 1
    base = _re.sub(r'\|\s*limit\s+\d+', '', dql, flags=_re.IGNORECASE).strip()
    fmt  = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    # Build probe DQL with limit 1 appended
    probe_dql = base + "\n| limit 1"
    try:
        result = ge.query(
            dql         = probe_dql,
            start       = fmt(start),
            end         = fmt(end),
            max_records = 1,
            strip_limit = False,  # already handled above
            stop_flag   = stop_flag,
        )
        return result["count"] > 0
    except Exception as e:
        log.warning(f"probe_has_data error [{fmt(start)}→{fmt(end)}]: {e}")
        return True  # safe fallback: assume data exists


def probe_schedule(dql: str, start: datetime, end: datetime,
                   granularity_h: float = 24.0,
                   append_log: Callable = None,
                   jid: str = None,
                   stop_flag=None) -> list:
    """
    Binary search profiler — finds windows with data using | limit 1 probes.
    Returns list of (start, end) tuples covering only ranges with data.
    Cost: O(N × log(range/granularity)) probes, each ~10-80MB.

    granularity_h: stop halving when window < this size (hours).
                   Typically = initial_window setting.
    """
    results = []
    probe_count = [0]

    def _log(msg):
        if append_log and jid:
            append_log(jid, {
                "type":  "log",
                "level": "info",
                "msg":   f"[probe] {msg}",
                "ts":    datetime.now(timezone.utc).isoformat(),
            })
        log.info(f"[probe] {msg}")

    def _search(s: datetime, e: datetime):
        if stop_flag and stop_flag.is_set():
            return
        probe_count[0] += 1
        hours = (e - s).total_seconds() / 3600
        fmt_h = lambda dt: dt.strftime("%Y-%m-%d %H:%M")

        has = probe_has_data(dql, s, e, stop_flag)
        _log(f"probe #{probe_count[0]}: {fmt_h(s)} → {fmt_h(e)} "
             f"({hours:.1f}h) → {'HAS DATA' if has else 'empty'}")

        if not has:
            return  # skip this range entirely

        if hours <= granularity_h:
            # Reached target granularity — add to schedule
            results.append((s, e))
            return

        # Has data and still too large — bisect
        mid = s + (e - s) / 2
        _search(s, mid)
        _search(mid, e)

    _log(f"Starting binary search profiler")
    _log(f"Range: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')} "
         f"({(end-start).total_seconds()/3600:.0f}h)")
    _log(f"Granularity: {granularity_h}h | "
         f"Max probes estimate: ~{int((end-start).total_seconds()/3600/granularity_h * 2)}")

    _search(start, end)

    _log(f"Profiling done: {probe_count[0]} probes → "
         f"{len(results)} windows with data")

    # Sort windows by start time — do NOT merge adjacent windows
    # Adjacent windows found by probing should stay separate for extraction
    # so each is processed independently. Merging collapses them back into
    # the full range which defeats the purpose of profiling.
    if not results:
        return []
    results.sort(key=lambda x: x[0])
    return results

# ── Top-level extraction orchestrator ─────────────────────────────────────────
def run_extraction(job: ExtractionJob):
    """
    Entry point. Builds initial windows from [start, end] using initial_hours,
    then recursively splits each one as needed.
    """
    job.log(f"🚀 Extraction started")
    job.log(f"   Range : {_human(job.start)} → {_human(job.end)}")
    job.log(f"   DQL   : {job.dql[:120]}{'...' if len(job.dql)>120 else ''}")
    job.log(f"   Cap   : {job.max_records:,} records | "
            f"max_depth={job.max_depth} | "
            f"initial_window={'full' if job.initial_hours==0 else str(job.initial_hours)+'h'}")
    job.log(f"   Output: {os.path.basename(job.outpath)} ({job.fmt.upper()})")

    # Build initial window list
    if job.probe:
        # Pre-flight binary search profiler
        granularity_h = job.initial_hours if job.initial_hours > 0 else 24.0
        job.log(f"   🔍 Pre-flight profiling enabled (granularity={granularity_h}h)")
        job.append_log(job.jid, {
            "type": "log", "level": "info",
            "msg": f"[probe] Starting binary search — finding windows with data...",
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        probed = probe_schedule(
            dql           = job.dql,
            start         = job.start,
            end           = job.end,
            granularity_h = granularity_h,
            append_log    = job.append_log,
            jid           = job.jid,
            stop_flag     = job.stop_flag,
        )
        if not probed:
            job.log("   ○ Profiling found no data in range — done")
            windows = []
        else:
            windows = probed
            job.log(f"   Profiling complete → {len(windows)} window(s) with data")
    elif job.initial_hours == 0:
        # Full range as single window
        windows = [(job.start, job.end)]
    else:
        # Slice into initial_hours chunks
        windows = []
        cur = job.start
        step = timedelta(hours=job.initial_hours)
        while cur < job.end:
            nxt = min(cur + step, job.end)
            windows.append((cur, nxt))
            cur = nxt

    job.log(f"   Windows: {len(windows)} initial window(s)")
    job.append_log(job.jid, {
        "type": "started",
        "windows": len(windows),
        "ts": datetime.now(timezone.utc).isoformat(),
    })

    t0 = time.time()
    job.windows_total   = len(windows)
    job.windows_done    = 0
    job.windows_current = 0  # 1-based index of window being processed

    for i, (ws, we) in enumerate(windows, 1):
        if job.stopped():
            break
        job.windows_current = i
        job.log(f"\n── Window {i}/{len(windows)} ──")
        _extract_window(job, ws, we, depth=0)
        job.windows_done = i
        # Inter-window delay — reduces Grail scheduler pressure
        if not job.stopped() and not job.pause_flag.is_set():
            time.sleep(2)

    elapsed = time.time() - t0
    status  = "stopped" if job.stopped() else "done"

    job.log(
        f"\n{'⏹ STOPPED' if job.stopped() else '✅ DONE'}: "
        f"{job.total_records:,} records | "
        f"{job.total_api_calls} API calls | "
        f"{job.total_splits} splits | "
        f"{job.errors} errors | "
        f"{job.warnings} warnings | "
        f"{elapsed/60:.2f}min"
    )

    job.append_log(job.jid, {
        "type": status,
        "total_records":   job.total_records,
        "total_api_calls": job.total_api_calls,
        "total_splits":    job.total_splits,
        "scanned_bytes":   job.total_scanned_b,
        "scanned_records": job.total_scanned_r,
        "errors":          job.errors,
        "warnings":        job.warnings,
        "elapsed_s":       round(elapsed, 1),
        "ts":              datetime.now(timezone.utc).isoformat(),
    })
