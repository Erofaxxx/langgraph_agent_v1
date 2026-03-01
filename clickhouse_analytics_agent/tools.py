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

    NOTE: The schema is already embedded in your system prompt — do NOT call this
    at the start of a session. Use it only if a table seems missing or the embedded
    schema appears incomplete.

    If you need exact column types, run: SELECT name, type FROM system.columns WHERE table='...' LIMIT 100

    Returns: JSON array of objects like:
      [{"table": "visits", "columns": ["date", "session_id", "revenue", ...]}, ...]
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
    Execute a SELECT SQL query against ClickHouse.

    Returns JSON with:
      - row_count: total rows fetched
      - columns: list of column names
      - dtypes: pandas dtypes of each column
      - preview_first_5_rows: first 5 rows as list of dicts
      - parquet_path: path to the saved Parquet file (pass this to python_analysis)

    IMPORTANT RULES:
    1. Only SELECT queries are allowed (no INSERT / UPDATE / DELETE / DROP).
    2. Always add a LIMIT — use 1000–10000 for analysis, up to 50000 for large exports.
    3. Push aggregations (SUM, COUNT, AVG, GROUP BY) into SQL — ClickHouse is extremely fast.
    4. For Array-type columns use arrayJoin() to explode them if needed.
    5. Save the parquet_path from the response; you MUST pass it to python_analysis.
    6. If you need the table size first, do: SELECT count() FROM table_name LIMIT 1.

    Args:
        sql: A valid ClickHouse SELECT statement.
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
    Execute Python code to analyze and visualize data from a ClickHouse query result.

    The data is pre-loaded from the Parquet file and available as `df` (pandas DataFrame).
    DO NOT call pd.read_parquet() in your code — `df` is already loaded.

    Available variables in the execution context:
      - df  (pd.DataFrame) — the data from ClickHouse
      - pd  (pandas)
      - np  (numpy)
      - plt (matplotlib.pyplot)
      - sns (seaborn)
      - result (None) — set this to a string for the final text/Markdown output

    CODE RULES:
    1. Set `result` to a Markdown string for the final text output shown to the user.
       Example: result = "## Sales\\n| Month | Revenue |\\n|..."
    2. Use print() for intermediate logging (e.g., print("Step 1: calculating CTR...")).
    3. Build charts with plt/sns — ALL matplotlib figures are automatically captured as PNG.
    4. Label chart titles, axes, and legends IN RUSSIAN.
    5. Format numbers with thousands separators: f"{value:,.0f}" or f"{value:,.2f}".
    6. Handle missing data: df.dropna() or df.fillna(0) before calculations.
    7. If the code raises an error, fix it and retry — do not abort the analysis.

    Advertising metrics to calculate when relevant:
      CTR  = clicks / impressions * 100
      CPC  = spend / clicks
      CPM  = spend / impressions * 1000
      ROAS = revenue / spend * 100
      CR   = conversions / clicks * 100

    Args:
        code: Python code. Variable `df` already contains the DataFrame.
        parquet_path: Parquet file path returned by clickhouse_query (field: parquet_path).

    Returns:
        Tuple of (content, artifact) where:
          - content: JSON string with success/output/result/error (sent to LLM)
          - artifact: list of base64 PNG data URIs (stored in state, NOT sent to LLM)
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
