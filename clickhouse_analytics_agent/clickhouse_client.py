"""
ClickHouse client for the Analytics Agent.
Executes SELECT queries and saves results to Parquet.
"""

import hashlib
import json
import time
from typing import Any

import clickhouse_connect
import numpy as np
import pandas as pd

from config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_SSL_CERT,
    CLICKHOUSE_USER,
    TEMP_DIR,
)


def _safe_json_value(v: Any) -> Any:
    """Convert numpy/pandas types and complex types to JSON-serializable values."""
    if isinstance(v, (list, dict, set, tuple)):
        return str(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v) if not np.isnan(v) else None
    if isinstance(v, np.ndarray):
        return v.tolist()
    # Check for pandas NA / NaT / NaN
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


class ClickHouseClient:
    """Direct connection to ClickHouse. Executes queries and saves data to Parquet."""

    def __init__(self) -> None:
        connect_kwargs: dict = {
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "username": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "database": CLICKHOUSE_DATABASE,
            "secure": True,
            "connect_timeout": 30,
            "send_receive_timeout": 300,
        }
        if CLICKHOUSE_SSL_CERT:
            connect_kwargs["verify"] = True
            connect_kwargs["ca_cert"] = CLICKHOUSE_SSL_CERT
        else:
            connect_kwargs["verify"] = False

        self.client = clickhouse_connect.get_client(**connect_kwargs)
        print(
            f"✅ ClickHouse connected: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
            f"/{CLICKHOUSE_DATABASE}"
        )

    def list_tables(self) -> list[dict]:
        """
        Return all tables in the current database with column names only (no types).
        Result: [{"table": "visits", "columns": ["id", "date", ...]}, ...]

        Types are omitted to reduce token usage (~64% smaller response).
        The model can infer types from column names or run a LIMIT 0 query if needed.
        """
        result = self.client.query(
            "SELECT table, name "
            "FROM system.columns "
            "WHERE database = currentDatabase() "
            "ORDER BY table, position"
        )
        tables: dict[str, list] = {}
        for row in result.result_rows:
            table_name, col_name = row[0], row[1]
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(col_name)
        return [{"table": t, "columns": cols} for t, cols in tables.items()]

    def execute_query(self, sql: str, limit: int = 50000) -> dict:
        """
        Execute a SELECT query.
        1. Validates it starts with SELECT.
        2. Appends LIMIT if missing.
        3. Runs query, builds DataFrame.
        4. Saves DataFrame to Parquet in TEMP_DIR.
        5. Returns metadata + first 5 rows preview + parquet_path.
        """
        sql_stripped = sql.strip()

        # Security: only SELECT allowed
        if not sql_stripped.upper().startswith("SELECT"):
            return {
                "success": False,
                "error": "Only SELECT queries are allowed. INSERT/UPDATE/DELETE/DROP are forbidden.",
            }

        # Auto-add LIMIT if missing
        if "LIMIT" not in sql_stripped.upper():
            sql_stripped = f"{sql_stripped.rstrip().rstrip(';')} LIMIT {limit}"

        try:
            result = self.client.query(sql_stripped)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)

            # Save to Parquet (preserves complex types like Array, Map, Decimal)
            query_hash = hashlib.md5(sql_stripped.encode()).hexdigest()[:10]
            parquet_filename = f"query_{query_hash}_{int(time.time())}.parquet"
            parquet_path = str(TEMP_DIR / parquet_filename)
            df.to_parquet(parquet_path, engine="pyarrow", index=False)

            # Build JSON-safe preview of first 5 rows
            preview = df.head(5).to_dict(orient="records")
            for row in preview:
                for k, v in row.items():
                    row[k] = _safe_json_value(v)

            return {
                "success": True,
                "row_count": len(df),
                "columns": list(df.columns),
                "dtypes": {col: str(df[col].dtype) for col in df.columns},
                "preview_first_5_rows": preview,
                "parquet_path": parquet_path,
            }

        except Exception as exc:
            return {
                "success": False,
                "error": str(exc),
                "sql": sql_stripped,
            }
