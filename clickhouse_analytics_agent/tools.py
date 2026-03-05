"""
LangGraph tool definitions for the Analytics Agent.

Three tools:
  1. list_tables      — discover ClickHouse schema
  2. clickhouse_query — run SELECT → save Parquet → return preview + path
  3. python_analysis  — exec Python with df loaded from Parquet, capture plots
"""

import json
from typing import Optional

from langchain_core.tools import tool

# ─── Lazy singletons ──────────────────────────────────────────────────────────
# Created on first use so config is loaded before connecting.
_ch_client = None
_sandbox = None


def _get_ch_client():
    global _ch_client
    if _ch_client is None:
        from clickhouse_client import ClickHouseClient
        _ch_client = ClickHouseClient()
    return _ch_client


def _get_sandbox():
    global _sandbox
    if _sandbox is None:
        from python_sandbox import PythonSandbox
        _sandbox = PythonSandbox()
    return _sandbox


# ─── Tool 1: list_tables ──────────────────────────────────────────────────────
@tool
def list_tables() -> str:
    """
    Get the list of ALL tables in the ClickHouse database with their column names.

    NOTE: The schema (with column types) is already embedded in your system prompt —
    do NOT call this at the start of a session. Use it only if a table seems missing
    or the embedded schema appears incomplete.

    Returns: JSON array of objects like:
      [{"table": "visits", "columns": [{"name": "date", "type": "Date"}, ...]}, ...]
    """
    try:
        tables = _get_ch_client().list_tables()
        return json.dumps(tables, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ─── Tool 2: clickhouse_query ─────────────────────────────────────────────────
@tool
def clickhouse_query(sql: str) -> str:
    """
    Execute a SELECT query against ClickHouse.

    Returns JSON with fields:
      - row_count: total rows returned
      - columns: list of column names
      - col_stats: per-column stats (type, min/max for numeric/datetime; unique+sample for strings)
      - parquet_path: path to saved Parquet file — pass this to python_analysis
      - cached: true if result was served from cache without hitting ClickHouse

    Rules: SELECT only; always include LIMIT; use WITH/CTE to join multiple tables in one query.

    Args:
        sql: ClickHouse SELECT statement.
    """
    try:
        result = _get_ch_client().execute_query(sql)
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as exc:
        return json.dumps({"success": False, "error": str(exc)})


# ─── Tool 3: python_analysis ──────────────────────────────────────────────────
@tool(response_format="content_and_artifact")
def python_analysis(code: str, parquet_path: str) -> tuple[str, list[str]]:
    """
    Execute Python to analyze data from a ClickHouse query result.

    `df` (pandas DataFrame) is pre-loaded — do NOT call pd.read_parquet().
    Available: df, pd, np, plt, sns, result=None.

    Rules:
    1. Set `result` to a Markdown string (shown to the user).
    2. Use print() for intermediate logging.
    3. All matplotlib figures are auto-captured as PNG.
    4. Label charts in Russian; format numbers with thousands separators.
    5. Handle missing data before calculations.

    Args:
        code: Python code. `df` is already loaded.
        parquet_path: Returned by clickhouse_query.
    """
    try:
        result = _get_sandbox().execute(code=code, parquet_path=parquet_path)
        plots: list[str] = result.pop("plots", [])
        content = json.dumps(result, ensure_ascii=False, default=str)
        return content, plots
    except Exception as exc:
        import traceback as tb
        full_tb = f"{exc}\n{tb.format_exc()}"
        content = json.dumps({
            "success": False,
            "output": "",
            "result": None,
            "error": full_tb[-1500:] if len(full_tb) > 1500 else full_tb,
        })
        return content, []


# ─── Exported list ────────────────────────────────────────────────────────────
TOOLS = [list_tables, clickhouse_query, python_analysis]
