"""
SegmentStore — thread-safe SQLite CRUD для именованных сегментов аудитории.

Сегменты хранятся в той же chat_history.db (отдельная таблица `segments`).
JSON-схема каждого сегмента соответствует segmentation_skill_spec.md §3–§5.

При переходе на RAG — только этот модуль меняется, агент не трогается.
"""

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

from config import DB_PATH


class SegmentStore:
    """Thread-safe CRUD для сегментов аудитории."""

    def __init__(self, db_path: str = DB_PATH) -> None:
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS segments (
                    segment_id      TEXT PRIMARY KEY,
                    name            TEXT NOT NULL UNIQUE,
                    description     TEXT,
                    approach        TEXT,
                    period_json     TEXT,
                    conditions_json TEXT,
                    primary_table   TEXT,
                    join_tables_json TEXT,
                    sql_query       TEXT,
                    last_count      INTEGER,
                    last_materialized TEXT,
                    used_in_json    TEXT,
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_segments_name ON segments(name);
            """)
            self._conn.commit()

    def save(self, segment: dict) -> dict:
        """Сохранить или обновить сегмент. Возвращает сохранённый объект."""
        now = datetime.now(timezone.utc).date().isoformat()
        seg_id = segment.get("segment_id") or f"seg_{uuid.uuid4().hex[:8]}"

        with self._lock:
            self._conn.execute(
                """
                INSERT INTO segments (
                    segment_id, name, description, approach,
                    period_json, conditions_json, primary_table,
                    join_tables_json, sql_query, last_count,
                    last_materialized, used_in_json, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(segment_id) DO UPDATE SET
                    name             = excluded.name,
                    description      = excluded.description,
                    approach         = excluded.approach,
                    period_json      = excluded.period_json,
                    conditions_json  = excluded.conditions_json,
                    primary_table    = excluded.primary_table,
                    join_tables_json = excluded.join_tables_json,
                    sql_query        = excluded.sql_query,
                    last_count       = excluded.last_count,
                    last_materialized= excluded.last_materialized,
                    used_in_json     = excluded.used_in_json,
                    updated_at       = excluded.updated_at
                """,
                (
                    seg_id,
                    segment["name"],
                    segment.get("description", ""),
                    segment.get("approach", ""),
                    json.dumps(segment.get("period", {}), ensure_ascii=False),
                    json.dumps(segment.get("conditions", {}), ensure_ascii=False),
                    segment.get("primary_table", ""),
                    json.dumps(segment.get("join_tables", []), ensure_ascii=False),
                    segment.get("sql_query", ""),
                    segment.get("last_count"),
                    segment.get("last_materialized", now),
                    json.dumps(segment.get("used_in", []), ensure_ascii=False),
                    segment.get("created_at", now),
                    now,
                ),
            )
            self._conn.commit()

        segment["segment_id"] = seg_id
        segment["updated_at"] = now
        if "created_at" not in segment:
            segment["created_at"] = now
        return segment

    def get_by_name(self, name: str) -> Optional[dict]:
        """Найти сегмент по имени (регистронезависимо)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE lower(name) = lower(?)", (name,)
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_by_id(self, segment_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE segment_id = ?", (segment_id,)
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_all(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM segments ORDER BY updated_at DESC"
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def delete(self, segment_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM segments WHERE segment_id = ?", (segment_id,)
            )
            self._conn.commit()
        return cur.rowcount > 0

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        d = dict(row)
        mapping = {
            "period_json":      "period",
            "conditions_json":  "conditions",
            "join_tables_json": "join_tables",
            "used_in_json":     "used_in",
        }
        for col_json, col_name in mapping.items():
            raw = d.pop(col_json, None)
            try:
                d[col_name] = json.loads(raw or "{}")
            except Exception:
                d[col_name] = {} if col_name != "join_tables" and col_name != "used_in" else []
        return d


# ─── Singleton ─────────────────────────────────────────────────────────────────
_store: Optional[SegmentStore] = None
_store_lock = threading.Lock()


def get_segment_store() -> SegmentStore:
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = SegmentStore()
    return _store
