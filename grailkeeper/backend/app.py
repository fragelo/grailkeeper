from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, Response
from pydantic import BaseModel
import subprocess, threading, queue, json, csv, io, os, re

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DTCTL = "/usr/local/bin/dtctl"
OUTDIR = "/tmp/dtctl-exports"
os.makedirs(OUTDIR, exist_ok=True)

DEBUG_KEYS = ["===> REQUEST", "===> RESPONSE", "STATUS:", "TIME:", "POST https://",
              "GET https://", '"state":', '"progress":', '"requestToken":', '"ttlSeconds":',
              '"scannedRecords":', '"scannedBytes":', '"executionTimeMilliseconds":', "Using config"]

def clean_debug(line):
    if not any(k in line for k in DEBUG_KEYS):
        return None
    if "===> REQUEST" in line: return "===> REQUEST ==="
    if "===> RESPONSE" in line: return "===> RESPONSE ==="
    if line.startswith("STATUS:"): return line.strip()
    if line.startswith("TIME:"): return line.strip()
    if line.startswith("POST https://") or line.startswith("GET https://"): return line.strip()
    if "Using config" in line: return line.strip()
    parts = []
    for k in ["state", "progress", "requestToken", "ttlSeconds", "scannedRecords", "scannedBytes", "executionTimeMilliseconds"]:
        m = re.search(f'"{k}":"?([^,"}}]*)"?', line)
        if m: parts.append(f'{k}: {m.group(1).strip(chr(34))}')
    return " | ".join(parts) if parts else None

def run_dtctl(args):
    try:
        r = subprocess.run([DTCTL]+args, capture_output=True, text=True, timeout=None, env={**os.environ})
        return {"stdout": r.stdout, "stderr": r.stderr, "returncode": r.returncode, "success": r.returncode==0}
    except FileNotFoundError:
        raise HTTPException(500, "dtctl not found")

class ConfigReq(BaseModel):
    environment: str
    token: str
    context_name: str = "default"

class CmdReq(BaseModel):
    args: list
    write_to_disk: bool = False
    output_filename: str = ""

class ExportReq(BaseModel):
    data: str
    mode: str = "full"
    fmt: str = "json"

@app.get("/api/health")
def health():
    r = run_dtctl(["--version"])
    return {"status": "ok", "dtctl": r["stdout"].strip()}

@app.post("/api/config")
def set_config(req: ConfigReq):
    r1 = run_dtctl(["config", "set-credentials", req.context_name, "--token", req.token])
    if not r1["success"]: raise HTTPException(400, r1["stderr"])
    r2 = run_dtctl(["config", "set-context", req.context_name,
                    "--environment", req.environment, "--token-ref", req.context_name])
    if not r2["success"]: raise HTTPException(400, r2["stderr"])
    run_dtctl(["ctx", req.context_name])
    return {"success": True}

@app.post("/api/command/stream")
def stream(req: CmdReq):
    outpath = None
    args = list(req.args)

    if req.write_to_disk and req.output_filename:
        safe = re.sub(r"[^a-zA-Z0-9._-]", "_", req.output_filename)
        outpath = os.path.join(OUTDIR, safe)
        # Strip -v/-vv: dtctl writes debug to stdout which would corrupt the file
        stripped = [a for a in args if a in ('-v', '-vv', '--debug')]
        args = [a for a in args if a not in ('-v', '-vv', '--debug')]
        if stripped:
            print(f"[disk mode] stripped flags that write to stdout: {stripped}")
        # Ensure -o is set, default to json
        if '-o' not in args and '--output' not in args:
            args.extend(['-o', 'json'])

    stripped_flags = [a for a in req.args if a in ('-v', '-vv', '--debug')] if outpath else []

    def generate():
        outfile = None
        try:
            if stripped_flags:
                msg = json.dumps({"type":"debug","line":"[disk mode] -v/-vv stripped to keep output file clean"})
                yield "data: " + msg + "\n\n"
            if outpath:
                outfile = open(outpath, "w")
                proc = subprocess.Popen([DTCTL]+args, stdout=outfile,
                                        stderr=subprocess.PIPE, text=True, env={**os.environ})
            else:
                proc = subprocess.Popen([DTCTL]+args, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, env={**os.environ})

            q2 = queue.Queue()
            stdout_lines = []
            stderr_lines = []

            def rd_stderr():
                for line in proc.stderr: q2.put(("err", line.rstrip()))
                q2.put(("done_err", None))

            def rd_stdout():
                if outpath: q2.put(("done_out", None)); return
                for line in proc.stdout: q2.put(("out", line.rstrip()))
                q2.put(("done_out", None))

            threading.Thread(target=rd_stderr, daemon=True).start()
            threading.Thread(target=rd_stdout, daemon=True).start()

            done = 0
            last_sz = 0
            while done < 2:
                try: kind, line = q2.get(timeout=0.5)
                except queue.Empty:
                    if outpath and os.path.exists(outpath):
                        sz = os.path.getsize(outpath)
                        if sz != last_sz:
                            last_sz = sz
                            yield f"data: {json.dumps({'type':'disk_progress','mb':round(sz/1024/1024,2)})}\n\n"
                    yield f"data: {json.dumps({'type':'ping'})}\n\n"
                    continue
                if kind in ("done_err","done_out"): done+=1; continue
                if kind == "out": stdout_lines.append(line)
                elif kind == "err":
                    stderr_lines.append(line)
                    clean = clean_debug(line)
                    if clean:
                        yield f"data: {json.dumps({'type':'debug','line':clean})}\n\n"

            proc.wait()
            if outfile: outfile.close()

            fileinfo = None
            if outpath and os.path.exists(outpath):
                sz = os.path.getsize(outpath)
                wc = subprocess.run(["wc","-l",outpath], capture_output=True, text=True)
                lc = int(wc.stdout.split()[0]) if wc.returncode==0 else 0
                fileinfo = {"filename": os.path.basename(outpath), "size_mb": round(sz/1024/1024,2),
                            "size_bytes": sz, "lines": lc}

            yield f"data: {json.dumps({'type':'done','returncode':proc.returncode,'success':proc.returncode==0,'stdout':chr(10).join(stdout_lines) if not outpath else '','stderr':chr(10).join(stderr_lines),'file':fileinfo})}\n\n"

        except Exception as e:
            if outfile:
                try: outfile.close()
                except: pass
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.get("/api/files")
def list_files():
    files = []
    for f in os.listdir(OUTDIR):
        fp = os.path.join(OUTDIR, f)
        s = os.stat(fp)
        files.append({"filename":f,"size_mb":round(s.st_size/1024/1024,2),"modified":s.st_mtime})
    return {"files": sorted(files, key=lambda x: x["modified"], reverse=True)}

@app.get("/api/files/{filename}/preview")
def preview(filename: str, lines: int = 100):
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)
    fp = os.path.join(OUTDIR, safe)
    if not os.path.exists(fp): raise HTTPException(404, "Not found")
    r = subprocess.run(["tail","-n",str(lines),fp], capture_output=True, text=True)
    wc = subprocess.run(["wc","-l",fp], capture_output=True, text=True)
    total = int(wc.stdout.split()[0]) if wc.returncode==0 else 0
    return {"filename":filename,"preview":r.stdout,"total_lines":total,
            "size_mb":round(os.path.getsize(fp)/1024/1024,2)}

@app.get("/api/files/{filename}/download")
def download(filename: str):
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)
    fp = os.path.join(OUTDIR, safe)
    if not os.path.exists(fp): raise HTTPException(404, "Not found")
    return FileResponse(fp, filename=filename, media_type="application/octet-stream")

@app.delete("/api/files/{filename}")
def delete_file(filename: str):
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)
    fp = os.path.join(OUTDIR, safe)
    if not os.path.exists(fp): raise HTTPException(404, "Not found")
    os.remove(fp)
    return {"success": True}

@app.post("/api/export/content")
def exp_content(req: ExportReq):
    try:
        lines = []
        if req.fmt == "json":
            p = json.loads(req.data)
            for r in (p.get("records") or p.get("result",{}).get("records") or []):
                lines.append(str(r.get("content") or r.get("log.content") or r.get("body") or ""))
        else:
            for row in csv.DictReader(io.StringIO(req.data)):
                lines.append(str(row.get("content") or row.get("log.content") or ""))
        return Response("\n".join(lines), media_type="text/plain",
                        headers={"Content-Disposition":"attachment; filename=content.log"})
    except Exception as e: raise HTTPException(400, str(e))

@app.post("/api/export/full")
def exp_full(req: ExportReq):
    try:
        if req.fmt == "json":
            p = json.loads(req.data)
            recs = p.get("records") or p.get("result",{}).get("records") or []
        else:
            recs = list(csv.DictReader(io.StringIO(req.data)))
        if not recs: raise HTTPException(400, "No records")
        flat = [{k:(json.dumps(v) if isinstance(v,(dict,list)) else v) for k,v in r.items()} for r in recs]
        keys = list(dict.fromkeys(k for r in flat for k in r))
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
        w.writeheader(); w.writerows(flat)
        return Response(buf.getvalue(), media_type="text/csv",
                        headers={"Content-Disposition":"attachment; filename=export.csv"})
    except HTTPException: raise
    except Exception as e: raise HTTPException(400, str(e))

@app.get("/api/workflows")
def workflows():
    r = run_dtctl(["get","workflows","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

@app.get("/api/dashboards")
def dashboards():
    r = run_dtctl(["get","dashboards","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

@app.get("/api/buckets")
def buckets():
    r = run_dtctl(["get","buckets","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}

@app.get("/api/slos")
def slos():
    r = run_dtctl(["get","slos","-o","json"])
    return {"success":r["success"],"output":r["stdout"],"error":r["stderr"]}


class ChunkRequest(BaseModel):
    dql_template: str          # DQL without timeframe, e.g. "fetch logs, scanLimitGBytes:-1"
    start: str                 # ISO timestamp e.g. "2025-03-15T00:00:00Z"
    end: str                   # ISO timestamp e.g. "2026-03-15T00:00:00Z"
    chunk_hours: int = 6       # Hours per chunk
    max_records_per_chunk: int = 500000
    output_filename: str = ""
    extra_args: list = []      # Additional dtctl flags


@app.post("/api/extract/chunked")
def extract_chunked(req: ChunkRequest):
    """Run chunked extraction, appending each chunk to a single file."""
    if not req.output_filename:
        raise HTTPException(400, "output_filename required")

    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", req.output_filename)
    outpath = os.path.join(OUTDIR, safe)

    def generate():
        try:
            from datetime import datetime, timezone, timedelta

            start_dt = datetime.fromisoformat(req.start.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(req.end.replace("Z", "+00:00"))
            chunk_delta = timedelta(hours=req.chunk_hours)

            total_hours = (end_dt - start_dt).total_seconds() / 3600
            total_chunks = int(total_hours / req.chunk_hours) + 1

            total_records = 0
            chunk_num = 0
            chunk_start = start_dt

            # Check if file exists and has content (resume support)
            resume_from = None
            if os.path.exists(outpath) and os.path.getsize(outpath) > 0:
                # Read last line to find last timestamp
                result = subprocess.run(["tail", "-c", "4096", outpath],
                                        capture_output=True, text=True)
                import re as re2
                ts_matches = re2.findall(r'"timestamp":"([^"]+)"', result.stdout)
                if ts_matches:
                    last_ts = ts_matches[-1]
                    try:
                        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                        # Resume from the chunk containing the last timestamp
                        chunks_done = int((last_dt - start_dt).total_seconds() / 3600 / req.chunk_hours)
                        chunk_start = start_dt + chunk_delta * chunks_done
                        chunk_num = chunks_done
                        msg = json.dumps({"type": "resume", "from": chunk_start.isoformat(),
                                          "chunks_done": chunks_done})
                        yield "data: " + msg + "\n\n"
                    except:
                        pass

            # Open file for append (or create)
            mode = "a" if chunk_num > 0 else "w"
            outfile = open(outpath, mode)

            try:
                while chunk_start < end_dt:
                    chunk_end = min(chunk_start + chunk_delta, end_dt)
                    chunk_num += 1

                    from_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    to_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")

                    # Normalize DQL and inject timeframe directly into DQL
                    # using fetch source parameters which accept ISO timestamps
                    dql = ' '.join(req.dql_template.split())

                    # Remove any existing from:/to: from the DQL template
                    import re as re2
                    dql = re2.sub(r',?\s*from:[^\s,|]+', '', dql)
                    dql = re2.sub(r',?\s*to:[^\s,|]+', '', dql)

                    # Inject chunk timeframe after the fetch source
                    # Timestamps must be quoted in DQL: from:"2025-01-01T00:00:00Z"
                    for src in ["fetch logs", "fetch events", "fetch spans",
                                "fetch bizevents", "fetch metrics"]:
                        if src in dql:
                            dql = dql.replace(src,
                                src + ', from:"' + from_str + '", to:"' + to_str + '"')
                            break

                    args = ["query", dql,
                            "--max-result-records", str(req.max_records_per_chunk),
                            "-o", "json",
                            "--fetch-timeout-seconds", "600"]
                    args.extend(req.extra_args)

                    # Send progress update
                    progress_pct = round((chunk_num / total_chunks) * 100, 1)
                    msg = json.dumps({
                        "type": "chunk_start",
                        "chunk": chunk_num,
                        "total": total_chunks,
                        "from": from_str,
                        "to": to_str,
                        "progress_pct": progress_pct,
                        "total_records": total_records
                    })
                    yield "data: " + msg + "\n\n"

                    # Run dtctl for this chunk
                    proc = subprocess.Popen(
                        [DTCTL] + args,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        env={**os.environ}
                    )
                    stdout, stderr = proc.communicate()

                    if proc.returncode != 0:
                        msg = json.dumps({
                            "type": "chunk_error",
                            "chunk": chunk_num,
                            "error": stderr.strip()[:500]
                        })
                        yield "data: " + msg + "\n\n"
                        chunk_start = chunk_end
                        continue

                    # Count records in this chunk
                    chunk_records = 0
                    try:
                        parsed = json.loads(stdout)
                        recs = parsed.get("records") or []
                        chunk_records = len(recs)
                        # Write each record as a JSON line
                        for rec in recs:
                            outfile.write(json.dumps(rec) + "\n")
                        outfile.flush()
                    except:
                        # If not valid JSON, write raw
                        outfile.write(stdout)
                        outfile.flush()

                    total_records += chunk_records

                    msg = json.dumps({
                        "type": "chunk_done",
                        "chunk": chunk_num,
                        "total": total_chunks,
                        "chunk_records": chunk_records,
                        "total_records": total_records,
                        "progress_pct": progress_pct,
                        "from": from_str,
                        "to": to_str
                    })
                    yield "data: " + msg + "\n\n"

                    chunk_start = chunk_end

            finally:
                outfile.close()

            # Final stats
            sz = os.path.getsize(outpath) if os.path.exists(outpath) else 0
            msg = json.dumps({
                "type": "extraction_done",
                "total_records": total_records,
                "total_chunks": chunk_num,
                "filename": safe,
                "size_mb": round(sz / 1024 / 1024, 2)
            })
            yield "data: " + msg + "\n\n"

        except Exception as e:
            yield "data: " + json.dumps({"type": "error", "message": str(e)}) + "\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# ── Ingest Proxy ──────────────────────────────────────────────────────────────
class IngestPayload(BaseModel):
    events: list

@app.post("/api/ingest/send")
def ingest_send(payload: IngestPayload):
    """Proxy ingest call through backend to avoid CORS."""
    import requests as req
    try:
        if not os.path.exists(INGEST_CONFIG_FILE):
            raise HTTPException(400, "Ingest not configured")
        with open(INGEST_CONFIG_FILE) as f:
            cfg = json.load(f)
        url = cfg.get("ingest_url","").rstrip("/")
        token = cfg.get("ingest_token","")
        if not url or not token:
            raise HTTPException(400, "Ingest URL or token missing")
        # Normalize URL: apps.dynatrace.com -> live.dynatrace.com
        import re as _re
        url = _re.sub(r'https://([^.]+)\.apps\.dynatrace\.com', r'https://\1.live.dynatrace.com', url)
        url = url.rstrip("/")
        if not url.endswith("/api/v2/logs/ingest"):
            url = url + "/api/v2/logs/ingest"
        resp = req.post(url,
            headers={"Authorization": "Api-Token " + token,
                     "Content-Type": "application/json; charset=utf-8"},
            json=payload.events,
            timeout=30)
        return {"status": resp.status_code, "ok": resp.status_code in (200,202,204), "body": resp.text[:500]}
    except req.exceptions.RequestException as e:
        raise HTTPException(502, str(e))


# ── Ingest Config ─────────────────────────────────────────────────────────────
INGEST_CONFIG_FILE = "/root/.config/dtctl/ingest_config.json"

class IngestConfig(BaseModel):
    ingest_url: str
    ingest_token: str

@app.post("/api/ingest-config")
def save_ingest_config(cfg: IngestConfig):
    try:
        os.makedirs(os.path.dirname(INGEST_CONFIG_FILE), exist_ok=True)
        with open(INGEST_CONFIG_FILE, "w") as f:
            json.dump({"ingest_url": cfg.ingest_url, "ingest_token": cfg.ingest_token}, f)
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/ingest-config")
def get_ingest_config():
    try:
        if os.path.exists(INGEST_CONFIG_FILE):
            with open(INGEST_CONFIG_FILE) as f:
                d = json.load(f)
            return {"success": True, "ingest_url": d.get("ingest_url",""), "ingest_token": d.get("ingest_token","")}
        return {"success": True, "ingest_url": "", "ingest_token": ""}
    except Exception as e:
        return {"success": False, "ingest_url": "", "ingest_token": ""}


app.mount("/static", StaticFiles(directory="/app/frontend"), name="static")

@app.get("/")
def root(): return FileResponse("/app/frontend/index.html")
