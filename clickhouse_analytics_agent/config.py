"""
Configuration for ClickHouse Analytics Agent.
Loaded from .env file at project root.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the same directory as this file
load_dotenv(Path(__file__).parent / ".env")

# ─── OpenRouter ──────────────────────────────────────────────────────────────
OPENROUTER_API_KEY: str = os.environ.get("OPENROUTER_API_KEY", "")
MODEL: str = os.environ.get("MODEL", "anthropic/claude-sonnet-4-6")
MAX_TOKENS: int = int(os.environ.get("MAX_TOKENS", "8192"))

# ─── ClickHouse ──────────────────────────────────────────────────────────────
CLICKHOUSE_HOST: str = (
    os.environ.get("CLICKHOUSE_HOST", "")
    .replace("https://", "")
    .replace("http://", "")
)
CLICKHOUSE_PORT: int = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CLICKHOUSE_USER: str = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE: str = os.environ.get("CLICKHOUSE_DATABASE", "default")

# SSL certificate for Yandex Cloud ClickHouse
CLICKHOUSE_SSL_CERT: str = ""
_ssl_path = os.environ.get("CLICKHOUSE_SSL_CERT_PATH", "")
if _ssl_path:
    _cert = Path(_ssl_path)
    if not _cert.is_absolute():
        _cert = Path(__file__).parent / _cert
    if _cert.exists():
        CLICKHOUSE_SSL_CERT = str(_cert.resolve())

# ─── Server ───────────────────────────────────────────────────────────────────
SERVER_URL: str = os.environ.get("SERVER_URL", "http://localhost:8000")
HOST: str = os.environ.get("HOST", "0.0.0.0")
PORT: int = int(os.environ.get("PORT", "8000"))

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR: Path = Path(__file__).parent
TEMP_DIR: Path = BASE_DIR / "temp_data"
TEMP_DIR.mkdir(exist_ok=True)
DB_PATH: str = str(BASE_DIR / "chat_history.db")

# ─── Limits ───────────────────────────────────────────────────────────────────
MAX_AGENT_ITERATIONS: int = int(os.environ.get("MAX_AGENT_ITERATIONS", "15"))
TEMP_FILE_TTL_SECONDS: int = int(os.environ.get("TEMP_FILE_TTL_SECONDS", "3600"))  # 1 hour
