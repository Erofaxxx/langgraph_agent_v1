"""
FastAPI server for ClickHouse Analytics Agent.

Endpoints:
  GET  /                          health check
  GET  /health                    health check (for monitoring)
  GET  /api/info                  service info
  POST /api/analyze               main endpoint — send a query, get analysis + charts
  POST /api/session/new           create a new session_id
  GET  /api/session/{session_id}  get session metadata
  GET  /api/chat-stats            database statistics

All state is persisted in SQLite via SqliteSaver (LangGraph checkpointer).
Each session is isolated by session_id = LangGraph thread_id.
"""

import asyncio
import uuid
from datetime import datetime
from typing import Optional

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
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # Restrict to your frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request / Response models ────────────────────────────────────────────────
class AnalyzeRequest(BaseModel):
    """Request body for POST /api/analyze."""
    query: str
    session_id: Optional[str] = None


class AnalyzeResponse(BaseModel):
    """Successful response from POST /api/analyze."""
    success: bool
    session_id: str
    text_output: str
    plots: list[str]        # base64 PNG data URIs: "data:image/png;base64,..."
    tool_calls: list[dict]  # [{tool: str, input: dict}, ...]
    error: Optional[str]
    timestamp: str


# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    """Initialize the agent and start background cleanup tasks."""
    # Import here so FastAPI starts fast and shows errors clearly
    from agent import get_agent

    agent = get_agent()  # triggers ClickHouse connection check

    # Background task: clean up expired Parquet files every 30 min
    async def _cleanup_loop() -> None:
        while True:
            await asyncio.sleep(1800)
            try:
                n = await asyncio.to_thread(agent.cleanup_temp_files)
                if n:
                    print(f"[cleanup] Removed {n} expired parquet file(s)")
            except Exception as exc:
                print(f"[cleanup] Error: {exc}")

    asyncio.create_task(_cleanup_loop())
    print(f"✅ ClickHouse Analytics Agent API started | {SERVER_URL}")


# ─── Health / Info endpoints ──────────────────────────────────────────────────
@app.get("/", summary="Health check")
async def root():
    return {
        "status": "online",
        "service": "ClickHouse Analytics Agent",
        "version": "1.0.0",
        "model": "Claude Sonnet 4.6 (via OpenRouter)",
        "docs": f"{SERVER_URL}/docs",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/health", summary="Health check for uptime monitors")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/info", summary="Service features")
async def api_info():
    return {
        "service": "ClickHouse Analytics Agent",
        "version": "1.0.0",
        "model": "anthropic/claude-sonnet-4-6 via OpenRouter",
        "framework": "LangGraph + SqliteSaver",
        "features": [
            "Запросы к ClickHouse с выгрузкой в Parquet",
            "Анализ данных на Python (pandas / numpy)",
            "Построение графиков (matplotlib / seaborn) → base64 PNG",
            "История диалога: SqliteSaver (SQLite, per session_id)",
            "Изоляция сессий — сессии не смешиваются",
            "Рекламная аналитика: CTR, CPC, CPM, ROAS, CR, CPA",
        ],
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/chat-stats", summary="Database statistics")
async def chat_stats():
    """Return SQLite database statistics."""
    import os
    from config import DB_PATH

    db_size = 0
    if os.path.exists(DB_PATH):
        db_size = round(os.path.getsize(DB_PATH) / (1024 * 1024), 3)

    return {
        "db_path": DB_PATH,
        "db_size_mb": db_size,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ─── Session endpoints ────────────────────────────────────────────────────────
@app.post("/api/session/new", summary="Create a new conversation session")
async def new_session():
    """
    Generate a fresh session_id. Use this before the first message
    to get an ID that ties the entire conversation together.
    """
    session_id = str(uuid.uuid4())
    return {
        "session_id": session_id,
        "created_at": datetime.utcnow().isoformat(),
    }


@app.get("/api/session/{session_id}", summary="Get session metadata")
async def get_session(session_id: str):
    """Return how many messages are stored in this session."""
    from agent import get_agent

    agent = get_agent()
    info = await asyncio.to_thread(agent.get_session_info, session_id)
    return info


# ─── Main analyze endpoint ────────────────────────────────────────────────────
@app.post(
    "/api/analyze",
    response_model=AnalyzeResponse,
    summary="Send an analytics query",
    description=(
        "Send a natural-language question or analytics request. "
        "The agent queries ClickHouse, runs Python analysis, and returns "
        "Markdown text + base64 PNG charts.\n\n"
        "**session_id** — reuse the same value across messages to maintain context. "
        "If omitted, a new session is created automatically."
    ),
)
async def analyze(request: AnalyzeRequest):
    """
    Main endpoint. Body:
    ```json
    {
      "query":      "Покажи CTR по кампаниям за январь 2025",
      "session_id": "abc-123"   // optional; omit for a new session
    }
    ```
    """
    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="'query' cannot be empty")

    # Use provided session_id or generate a new one
    session_id = (request.session_id or "").strip() or str(uuid.uuid4())

    from agent import get_agent

    agent = get_agent()

    # Run the (synchronous) LangGraph agent in a thread pool so we don't
    # block the FastAPI event loop while the agent calls the LLM + tools.
    result = await asyncio.to_thread(agent.analyze, query, session_id)

    result["timestamp"] = datetime.utcnow().isoformat()

    if not result.get("success"):
        # Return 500 but still include partial results (plots captured so far)
        return JSONResponse(status_code=500, content=result)

    return JSONResponse(content=result)


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(
        f"""
╔══════════════════════════════════════════════════════════════╗
║       ClickHouse Analytics Agent API  v1.0                   ║
║       Model : Claude Sonnet 4.6 via OpenRouter               ║
║       Graph : LangGraph ReAct + SqliteSaver                  ║
╚══════════════════════════════════════════════════════════════╝

  Host  : {HOST}
  Port  : {PORT}
  Docs  : http://{HOST}:{PORT}/docs
  Health: http://{HOST}:{PORT}/health
"""
    )
    uvicorn.run(
        "api_server:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
    )
