"""
grail_engine.py — Grailkeeper v3.0
Pure-Python Grail Query API client. No dtctl dependency anywhere.

Key design:
- Any notification in the API response triggers truncation → window split
- maxResultBytes is NEVER sent (forces full scan)
- Credentials stored in memory + persisted to disk config
- All metadata exposed: scannedBytes, scannedRecords, notifications, queryId, executionTimeMs
"""

import requests
import tempfile
import ijson
import os
import threading
import logging
import time
import json
import re

from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("grail_engine")

# ── Constants ──────────────────────────────────────────────────────────────────
POLL_INTERVAL_S    = 5
POLL_MAX_WAIT_S    = 580
FETCH_TIMEOUT_S    = 60
# requestTimeoutMilliseconds intentionally omitted — causes constraint violations

# Any notification type from Grail means the result is incomplete
TRUNC_NOTIF_TYPES = {
    "LIMIT_ADDED",
    "FETCH_TIMEOUT",
    "SCAN_LIMIT_REACHED",
    "RESULT_TRUNCATED",
    "QUERY_TRUNCATED",
    "ANALYSIS_TIMEFRAME_ADAPTED",
    "FIELD_LIMIT_REACHED",
}

# ── Credential store ───────────────────────────────────────────────────────────
_cred_lock = threading.Lock()
_cred: dict = {}

def set_credentials(env_url: str, token: str):
    global _cred
    with _cred_lock:
        _cred = {
            "env_url": env_url.rstrip("/"),
            "token":   token,
        }

def get_credentials() -> dict:
    with _cred_lock:
        return dict(_cred)

def has_credentials() -> bool:
    c = get_credentials()
    return bool(c.get("env_url") and c.get("token"))

def _base_url() -> str:
    c = get_credentials()
    if not c.get("env_url"):
        raise RuntimeError("Grail credentials not configured. Use /api/config.")
    return c["env_url"] + "/platform/storage/query/v1"

def _headers() -> dict:
    c = get_credentials()
    return {
        "Authorization": f"Bearer {c['token']}",
        "Content-Type":  "application/json",
    }

# ── HTTP helpers ───────────────────────────────────────────────────────────────
def _parse_grail_response(tmp_path: str) -> dict:
    """
    Stream-parse a Grail poll response from a temp file using ijson.
    Streams records to a separate JSONL temp file to avoid holding
    500k records in RAM simultaneously.
    """
    size_mb = os.path.getsize(tmp_path) // 1024 // 1024
    log.info(f"[grail] parsing response from {tmp_path} ({size_mb}MB)")

    result = {
        "state": "SUCCEEDED",
        "result": {
            "records": [],
            "notifications": [],
            "metadata": {},
            "scanned_bytes": 0,
            "scanned_records": 0,
            "query_ms": 0,
        }
    }

    # Pass 1: stream records to a JSONL temp file — never hold all in RAM
    records_tmp = tmp_path + '.records.jsonl'
    record_count = 0
    try:
        with open(tmp_path, 'rb') as fin, open(records_tmp, 'w', encoding='utf-8') as fout:
            for record in ijson.items(fin, 'result.records.item'):
                fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                record_count += 1
        log.info(f"[grail] parsed {record_count} records from temp file")
    except Exception as e:
        log.warning(f"[grail] ijson record parse failed: {e} — falling back to full load")
        if os.path.exists(records_tmp): os.unlink(records_tmp)
        with open(tmp_path, 'rb') as f2:
            return json.load(f2)

    # Pass 2: parse metadata, notifications, scanned stats (small fields)
    # Paths match what query() reads from grail_meta dict after json.load
    grail_meta_parsed = {}
    try:
        with open(tmp_path, 'rb') as f:
            for prefix, event, value in ijson.parse(f):
                if prefix == 'result.metadata.grail.notifications.item.notificationType':
                    result["result"]["notifications"].append({"notificationType": value})
                elif prefix == 'result.metadata.grail.scannedBytes' and event in ('number', 'integer'):
                    grail_meta_parsed["scannedBytes"] = value
                elif prefix == 'result.metadata.grail.scannedRecords' and event in ('number', 'integer'):
                    grail_meta_parsed["scannedRecords"] = value
                elif prefix == 'result.metadata.grail.executionTimeMilliseconds' and event in ('number', 'integer'):
                    grail_meta_parsed["executionTimeMilliseconds"] = value
                elif prefix == 'result.metadata.grail.queryId' and event == 'string':
                    grail_meta_parsed["queryId"] = value
        result["result"]["grail_meta"] = grail_meta_parsed
        log.debug(f"[grail] metadata parsed: scannedBytes={grail_meta_parsed.get('scannedBytes',0)} "
                  f"scannedRecords={grail_meta_parsed.get('scannedRecords',0)} "
                  f"execMs={grail_meta_parsed.get('executionTimeMilliseconds',0)}")
    except Exception as e:
        log.warning(f"[grail] metadata parse failed: {e}")

    # Don't load records into RAM — return the JSONL path for direct streaming
    # Caller is responsible for reading and deleting records_tmp
    result["result"]["records"] = []
    result["result"]["records_jsonl_path"] = records_tmp
    result["result"]["record_count"] = record_count
    return result


def _post_query(payload: dict,
                stop_flag: Optional[threading.Event] = None) -> dict:
    """POST /query:execute, poll until SUCCEEDED. Returns full raw API response."""
    session = requests.Session()
    session.headers.update(_headers())
    url = _base_url() + "/query:execute"

    log.info(f"[grail] POST {url[:60]} query={payload.get('query','')[:60]}")
    t0 = time.time()
    try:
        r = session.post(url, json=payload, timeout=(10, 30))  # (connect, read) — POST returns token fast
        log.info(f"[grail] POST done in {time.time()-t0:.1f}s status={r.status_code}")
    except requests.exceptions.Timeout:
        log.error(f"[grail] POST TIMEOUT after {time.time()-t0:.1f}s — Grail not responding")
        raise RuntimeError(f"Grail POST timeout after 30s — connection hung")
    except requests.exceptions.ConnectionError as e:
        log.error(f"[grail] POST CONNECTION ERROR after {time.time()-t0:.1f}s: {e}")
        raise RuntimeError(f"Connection error reaching Grail: {e}")

    _raise_for_status(r)
    data = r.json()

    # Immediate result (HTTP 200, no polling needed)
    if "requestToken" not in data:
        return data

    # Async result — poll
    request_token = data["requestToken"]
    ttl           = data.get("ttlSeconds", 60)
    deadline      = time.time() + min(ttl - 10, POLL_MAX_WAIT_S)
    poll_url      = _base_url() + "/query:poll"
    t0            = time.time()  # track total elapsed for debug logging

    # Store token so external cancel is possible
    if stop_flag and hasattr(stop_flag, '_grail_token_ref'):
        stop_flag._grail_token_ref['token']   = request_token
        stop_flag._grail_token_ref['session']  = session
        stop_flag._grail_token_ref['base_url'] = _base_url()

    first_poll = True
    while time.time() < deadline:
        if stop_flag and stop_flag.is_set():
            _cancel(request_token, session)
            raise InterruptedError("Stopped by user")

        # Sleep in small increments so stop_flag is checked frequently
        sleep_total = 0.5 if first_poll else POLL_INTERVAL_S
        sleep_step  = 0.2  # check stop flag every 200ms
        log.debug(f"[grail] waiting {sleep_total}s before poll (token={request_token[:16]}...)")
        elapsed_sleep = 0.0
        while elapsed_sleep < sleep_total:
            if stop_flag and stop_flag.is_set():
                _cancel(request_token, session)
                raise InterruptedError("Stopped by user")
            time.sleep(min(sleep_step, sleep_total - elapsed_sleep))
            elapsed_sleep += sleep_step
        first_poll = False

        t_poll = time.time()
        log.info(f"[grail] POLL token={request_token[:16]}... elapsed={t_poll-t0:.1f}s")
        tmp_path = None
        total_bytes = 0
        CHUNK_TIMEOUT = 90  # if no data for 90s → stall detected
        last_chunk_time = [time.time()]
        stall_detected = [False]
        request_aborted = [False]

        # Start watchdog BEFORE the GET so it catches connection hangs too
        def watchdog():
            log.debug(f"[watchdog] started for token={request_token[:16]}...")
            while not stall_detected[0]:
                time.sleep(5)
                idle = time.time() - last_chunk_time[0]
                log.debug(f"[watchdog] idle={idle:.0f}s bytes_so_far={total_bytes} token={request_token[:16]}...")
                if idle > CHUNK_TIMEOUT:
                    log.error(f"[watchdog] STALL DETECTED — no data for {idle:.0f}s on token={request_token[:16]}... — aborting")
                    stall_detected[0] = True
                    request_aborted[0] = True
                    # Force close session AND create new one for future requests
                    try:
                        session.close()
                        # Also set a flag so the caller knows to create new session
                    except Exception as we:
                        log.warning(f"[watchdog] session close error: {we}")
                    # As last resort, raise exception in main thread via os.kill
                    import signal, os as _os
                    try:
                        _os.kill(_os.getpid(), signal.SIGALRM)
                    except (AttributeError, OSError):
                        pass  # SIGALRM not available on Windows
                    return
            log.debug(f"[watchdog] stopped for token={request_token[:16]}...")

        import threading as _threading
        wd = _threading.Thread(target=watchdog, daemon=True)
        wd.start()

        try:
            # Stream response to temp file — avoids loading 1GB+ into RAM
            r = session.get(poll_url,
                            params={"request-token": request_token},
                            timeout=(10, CHUNK_TIMEOUT),
                            stream=True)
            last_chunk_time[0] = time.time()  # reset after headers received

            with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as tmp:
                tmp_path = tmp.name
                for chunk in r.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    if stall_detected[0]:
                        break
                    if chunk:
                        tmp.write(chunk)
                        total_bytes += len(chunk)
                        last_chunk_time[0] = time.time()
                    if stop_flag and stop_flag.is_set():
                        stall_detected[0] = True
                        r.close()
                        raise InterruptedError("Stopped by user")
            stall_detected[0] = True  # stop watchdog
            if request_aborted[0]:
                raise RuntimeError(f"Grail poll stalled — no data for {CHUNK_TIMEOUT}s, session closed by watchdog")
            log.info(f"[grail] POLL done in {time.time()-t_poll:.1f}s status={r.status_code} bytes={total_bytes}")
        except InterruptedError:
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            raise
        except requests.exceptions.Timeout:
            log.error(f"[grail] POLL TIMEOUT after {time.time()-t_poll:.1f}s — no data for 60s")
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            raise RuntimeError(f"Grail poll timeout — connection stalled for 60s")
        except Exception as e:
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            raise

        _raise_for_status(r)

        # Handle empty body
        if total_bytes == 0:
            log.warning(f"[grail] POLL returned 200 with empty body — treating as RUNNING, will retry")
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            continue

        # Peek at first 200 bytes to check state
        with open(tmp_path, 'rb') as f:
            peek = f.read(200).decode('utf-8', errors='replace')
        log.info(f"[grail] POLL response bytes={total_bytes} content={peek[:120]}")

        # Parse state using streaming JSON — no full load into RAM for state check
        state = None
        progress = 0
        with open(tmp_path, 'rb') as f:
            for prefix, event, value in ijson.parse(f):
                if prefix == 'state' and event == 'string':
                    state = value
                elif prefix == 'progress' and event == 'number':
                    progress = value
                if state is not None:
                    break
        log.debug(f"Poll state={state} progress={progress}%")

        if state == "RUNNING":
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            continue
        if state == "FAILED":
            with open(tmp_path, 'rb') as f:
                data = json.load(f)
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            err = data.get("error", {})
            raise RuntimeError(f"Grail FAILED: {err.get('message')} | errorType={err.get('details',{}).get('errorType')}")
        if state != "SUCCEEDED":
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            continue

        # SUCCEEDED — stream-parse result from temp file to avoid loading 1GB+ into RAM
        data = _parse_grail_response(tmp_path)
        if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
        return data

    raise TimeoutError(f"Poll TTL expired for requestToken={request_token}")


def _raise_for_status(r: requests.Response):
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", 30))
        log.warning(f"429 rate-limit — sleeping {wait}s")
        time.sleep(wait)
        return
    if r.status_code in (500, 503):
        log.warning(f"{r.status_code} server error — sleeping 15s")
        time.sleep(15)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            detail = r.json()
            err    = detail.get("error", {})
            msg    = err.get("message") or detail.get("message") or str(e)
            # Include constraint violation details so we know exactly what failed
            violations = (err.get("details", {}) or {}).get("constraintViolations", [])
            if violations:
                vstr = "; ".join(
                    f"{v.get('path','?')}={v.get('message','?')}"
                    for v in violations
                )
                msg = f"{msg} | violations: {vstr}"
        except Exception:
            msg = str(e)
        raise RuntimeError(f"HTTP {r.status_code}: {msg}")


def _cancel(token: str, session: requests.Session):
    try:
        session.post(_base_url() + "/query:cancel",
                     params={"request-token": token},
                     timeout=10)
    except Exception:
        pass


# ── DQL helpers ────────────────────────────────────────────────────────────────
def _strip_limit(dql: str) -> str:
    """
    Clean DQL for extraction:
    - Remove | limit N only — extraction fetches all records via windowing
    - scanLimitGBytes is kept as-is — user controls scan behavior
    """
    dql = re.sub(r'\|\s*limit\s+\d+', '', dql, flags=re.IGNORECASE)
    return dql.strip()


def _inject_timeframe(dql: str, start: str, end: str) -> str:
    """
    Inject from:/to: directly into the DQL fetch statement.
    This scopes the Grail scan at the DQL engine level — not just the API payload.
    Without this, scanLimitGBytes:-1 scans the entire bucket on every split window.

    Example:
      IN:  fetch logs, scanLimitGBytes:-1
      OUT: fetch logs, from:"2026-01-18T16:47:00.000Z", to:"2026-01-18T22:47:00.000Z", scanLimitGBytes:-1

    Matches: fetch <datatype> [, existing_params]
    """
    # Match "fetch <word>" at start of DQL (case insensitive)
    m = re.match(r'(fetch\s+\w+)(.*)', dql.strip(), re.IGNORECASE | re.DOTALL)
    if not m:
        return dql  # fallback: DQL doesn't start with fetch, leave as-is
    fetch_part   = m.group(1)  # e.g. "fetch logs"
    rest         = m.group(2)  # e.g. ", scanLimitGBytes:-1\n| sort timestamp desc"
    # Insert from/to right after "fetch <datatype>"
    injected = f'{fetch_part}, from:"{start}", to:"{end}"{rest}'
    return injected


# ── Core query ─────────────────────────────────────────────────────────────────
def query(dql: str,
          start: str,
          end:   str,
          max_records: int = 1000,
          strip_limit: bool = False,
          stop_flag:   Optional[threading.Event] = None) -> dict:
    """
    Execute one DQL query against the Grail API.

    Returns dict:
        records          list of record dicts
        count            len(records)
        truncated        bool — True if any notification present
        notifications    list of notification dicts from Grail
        trunc_reasons    list of notificationType strings
        scanned_bytes    int
        scanned_records  int
        query_id         str
        execution_ms     int
        raw_metadata     full grail metadata dict
    """
    # Timeframe passed via defaultTimeframeStart/End API params only (same as DT Logs app)
    # Injecting from:/to: inline causes Grail to use slower index path vs fast bulk scan
    dql_exec = _strip_limit(dql) if strip_limit else dql

    payload = {
        "query":               dql_exec,
        "defaultTimeframeStart": start,
        "defaultTimeframeEnd":   end,
        "maxResultRecords":    max_records,
        "fetchTimeoutSeconds": 240,             # soft limit — keeps in Grail's guaranteed zone (<5min)
        # requestTimeoutMilliseconds omitted — causes constraint violations
        # maxResultBytes omitted — forces full scan
    }

    data       = _post_query(payload, stop_flag)
    result          = data.get("result", {})
    records         = result.get("records", []) or []
    records_jsonl   = result.get("records_jsonl_path")   # set by streaming parser
    record_count    = result.get("record_count", len(records))
    # grail_meta comes from streaming parser (grail_meta_parsed) or full json.load metadata
    grail_meta = result.get("grail_meta") or result.get("metadata", {}).get("grail", {})

    notifs      = grail_meta.get("notifications", []) or []
    sc_bytes    = int(grail_meta.get("scannedBytes",              0) or 0)
    sc_recs     = int(grail_meta.get("scannedRecords",            0) or 0)
    query_id    = grail_meta.get("queryId",                       "")
    exec_ms     = int(grail_meta.get("executionTimeMilliseconds", 0) or 0)

    # Truncation: any notification OR count == max_records
    trunc_reasons = [n.get("notificationType", "UNKNOWN")
                     for n in notifs
                     if n.get("notificationType")]
    truncated = bool(trunc_reasons) or record_count >= max_records

    return {
        "records":         records,
        "records_jsonl":   records_jsonl,
        "count":           record_count,
        "truncated":       truncated,
        "notifications":   notifs,
        "trunc_reasons":   trunc_reasons,
        "scanned_bytes":   sc_bytes,
        "scanned_records": sc_recs,
        "query_id":        query_id,
        "execution_ms":    exec_ms,
        "raw_metadata":    grail_meta,
    }


# ── Health check ───────────────────────────────────────────────────────────────
def health_check() -> dict:
    """
    Verify API connectivity and token validity only.
    fetch logs | limit 0 returns immediately with 0 records — no scan, no data needed.
    """
    if not has_credentials():
        return {"ok": False, "error": "No credentials configured"}
    try:
        from datetime import timedelta as _td
        now   = datetime.now(timezone.utc)
        start = now - _td(minutes=1)
        r = query(
            dql         = "fetch logs | limit 0",
            start       = start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end         = now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            max_records = 1,
        )
        return {"ok": True, "query_id": r["query_id"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}
