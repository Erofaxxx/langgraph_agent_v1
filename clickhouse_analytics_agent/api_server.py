"""
FastAPI server for ClickHouse Analytics Agent.
Endpoints:
  GET  /                              health check
  GET  /health                        health check (for monitoring)
  GET  /api/info                      service info
  POST /api/session/new               create a new conversation session
  GET  /api/session/{session_id}      get session metadata
  POST /api/analyze                   submit query → returns job_id immediately
  GET  /api/job/{job_id}              poll job status / get result
  GET  /api/chat-stats                database statistics
Architecture change: async job queue.
  - POST /api/analyze starts the agent in background, returns job_id instantly.
  - GET  /api/job/{job_id} returns status: "pending" | "running" | "done" | "error"
  - Results are kept in memory for 2 hours (JOB_TTL_SECONDS).
  - Client reconnecting after disconnect can still fetch the result.
"""
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional, Literal
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
load_dotenv()
from config import HOST, PORT, SERVER_URL
# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="ClickHouse Analytics Agent API",
    description=(
        "AI-powered advertising analytics agent. "
        "Queries ClickHouse, analyzes data with Python, returns charts & tables."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)
# ─── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ─── Job store ────────────────────────────────────────────────────────────────
# job_id → JobRecord dict
# Хранится в памяти; при рестарте сервера задачи теряются (это приемлемо).
JOB_TTL_SECONDS = 7200  # 2 часа
JobStatus = Literal["pending", "running", "done", "error"]
_jobs: dict[str, dict] = {}
def _new_job(session_id: str, query: str) -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "session_id": session_id,
        "query": query,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "finished_at": None,
        "result": None,   # AnalyzeResponse dict when done
        "error": None,
    }
    return job_id
def _set_running(job_id: str) -> None:
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = datetime.now(timezone.utc).isoformat()
def _set_done(job_id: str, result: dict) -> None:
    _jobs[job_id]["status"] = "done"
    _jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()
    _jobs[job_id]["result"] = result
def _set_error(job_id: str, error: str) -> None:
    _jobs[job_id]["status"] = "error"
    _jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()
    _jobs[job_id]["error"] = error
# ─── Request / Response models ────────────────────────────────────────────────
class AnalyzeRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
class SubmitResponse(BaseModel):
    """Returned immediately after POST /api/analyze."""
    job_id: str
    session_id: str
    status: str   # always "pending"
    message: str
class JobStatusResponse(BaseModel):
    """Returned by GET /api/job/{job_id}."""
    job_id: str
    session_id: str
    status: JobStatus
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    # Present only when status == "done"
    success: Optional[bool] = None
    text_output: Optional[str] = None
    plots: Optional[list[str]] = None
    tool_calls: Optional[list[dict]] = None
    error: Optional[str] = None
# ─── Background worker ────────────────────────────────────────────────────────
async def _run_agent_job(job_id: str) -> None:
    """Run the agent in a thread pool and store the result in _jobs."""
    job = _jobs.get(job_id)
    if not job:
        return
    _set_running(job_id)
    try:
        from agent import get_agent
        agent = get_agent()
        result = await asyncio.to_thread(
            agent.run,
            query=job["query"],
            session_id=job["session_id"],
        )
        _set_done(job_id, result)
    except Exception as exc:
        _set_error(job_id, str(exc))
        print(f"[job:{job_id}] ERROR: {exc}")
# ─── Cleanup loop ─────────────────────────────────────────────────────────────
async def _cleanup_loop() -> None:
    """Remove expired jobs and parquet files every 30 minutes."""
    while True:
        await asyncio.sleep(1800)
        now = datetime.now(timezone.utc).timestamp()
        # Clean expired jobs
        expired = [
            jid for jid, j in list(_jobs.items())
            if j["status"] in ("done", "error")
            and j["finished_at"]
            and (now - datetime.fromisoformat(j["finished_at"]).timestamp()) > JOB_TTL_SECONDS
        ]
        for jid in expired:
            del _jobs[jid]
        if expired:
            print(f"[cleanup] Removed {len(expired)} expired job(s)")
        # Clean parquet files
        try:
            from agent import get_agent
            n = await asyncio.to_thread(get_agent().cleanup_temp_files)
            if n:
                print(f"[cleanup] Removed {n} expired parquet file(s)")
        except Exception as exc:
            print(f"[cleanup] Parquet cleanup error: {exc}")
# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    from agent import get_agent
    get_agent()  # warm up: connect to ClickHouse
    asyncio.create_task(_cleanup_loop())
    print(f"✅ ClickHouse Analytics Agent API v2 started | {SERVER_URL}")
# ─── Health / Info ─────────────────────────────────────────────────────────────
@app.get("/", summary="Health check")
async def root():
    return {"status": "online", "service": "ClickHouse Analytics Agent", "version": "2.0.0"}
@app.get("/health", summary="Health check for uptime monitors")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
@app.get("/api/info", summary="Service features")
async def info():
    return {
        "service": "ClickHouse Analytics Agent",
        "version": "2.0.0",
        "architecture": "async job queue",
        "endpoints": {
            "submit": "POST /api/analyze",
            "poll":   "GET  /api/job/{job_id}",
        },
    }
# ─── Session endpoints ─────────────────────────────────────────────────────────
@app.post("/api/session/new", summary="Create a new conversation session")
async def new_session():
    session_id = str(uuid.uuid4())
    return {
        "session_id": session_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "message": "New session created",
    }
@app.get("/api/session/{session_id}", summary="Get session metadata")
async def get_session(session_id: str):
    # Count pending/running jobs for this session
    active = [j for j in _jobs.values() if j["session_id"] == session_id and j["status"] in ("pending", "running")]
    return {
        "session_id": session_id,
        "active_jobs": len(active),
    }
# ─── Main: submit query ────────────────────────────────────────────────────────
@app.post("/api/analyze", response_model=SubmitResponse, summary="Submit an analytics query")
async def analyze(req: AnalyzeRequest):
    """
    Submit a query to the agent.
    Returns job_id immediately — agent runs in background.
    Poll GET /api/job/{job_id} to get the result.
    """
    session_id = req.session_id or str(uuid.uuid4())
    job_id = _new_job(session_id=session_id, query=req.query)
    # Fire and forget
    asyncio.create_task(_run_agent_job(job_id))
    return SubmitResponse(
        job_id=job_id,
        session_id=session_id,
        status="pending",
        message="Query accepted. Poll GET /api/job/{job_id} for result.",
    )
# ─── Poll job status ───────────────────────────────────────────────────────────
@app.get("/api/job/{job_id}", response_model=JobStatusResponse, summary="Poll job status / get result")
async def get_job(job_id: str):
    """
    Poll the status of a submitted job.
    status: "pending" | "running" | "done" | "error"
    When status == "done", text_output, plots, tool_calls are populated.
    """
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found (may have expired)")
    resp = JobStatusResponse(
        job_id=job["job_id"],
        session_id=job["session_id"],
        status=job["status"],
        created_at=job["created_at"],
        started_at=job["started_at"],
        finished_at=job["finished_at"],
        error=job["error"],
    )
    if job["status"] == "done" and job["result"]:
        r = job["result"]
        resp.success = r.get("success", True)
        resp.text_output = r.get("text_output", "")
        resp.plots = r.get("plots", [])
        resp.tool_calls = r.get("tool_calls", [])
        resp.error = r.get("error")
    return resp
# ─── Stats ────────────────────────────────────────────────────────────────────
@app.get("/api/chat-stats", summary="Database statistics")
async def chat_stats():
    total = len(_jobs)
    by_status = {}
    for j in _jobs.values():
        by_status[j["status"]] = by_status.get(j["status"], 0) + 1
    return {"total_jobs_in_memory": total, "by_status": by_status}
# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("api_server:app", host=HOST, port=PORT, log_level="info")
