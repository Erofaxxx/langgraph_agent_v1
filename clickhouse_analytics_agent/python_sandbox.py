"""
Python code execution sandbox for the Analytics Agent.

Loads data from a Parquet file into a pandas DataFrame and executes
user-provided code in a controlled namespace. Captures:
  - stdout (print statements)
  - matplotlib/seaborn figures → base64 PNG strings
  - `result` variable → final text/table output
"""

import base64
import contextlib
import io
import traceback

# IMPORTANT: set non-interactive backend BEFORE importing pyplot
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# ─── Global matplotlib / seaborn settings ─────────────────────────────────────
plt.rcParams["figure.figsize"] = (12, 7)
plt.rcParams["figure.dpi"] = 100
plt.rcParams["font.size"] = 12
plt.rcParams["axes.unicode_minus"] = False  # Fix minus sign rendering

# Try to enable Cyrillic fonts if available
try:
    import matplotlib.font_manager as fm

    # Try DejaVu which has decent Unicode coverage
    plt.rcParams["font.family"] = "DejaVu Sans"
except Exception:
    pass

sns.set_style("whitegrid")
sns.set_palette("husl")


class PythonSandbox:
    """
    Executes Python analysis code with data pre-loaded from a Parquet file.
    Claude writes code that works with `df` — the sandbox handles parquet loading.
    """

    def execute(self, code: str, parquet_path: str) -> dict:
        """
        Execute Python code with data from parquet_path.

        The code receives:
          - df (pd.DataFrame): data from parquet
          - pd, np, plt, sns: pre-imported libraries
          - result (None → set by code for final text output)

        Returns:
          {
            "success": bool,
            "output": str,        # stdout from print()
            "result": str | None, # value of `result` variable (Markdown text/table)
            "plots": list[str],   # base64 PNG data URIs
            "error": str | None,
          }
        """
        # ── Load data from Parquet ──────────────────────────────────────────
        try:
            df = pd.read_parquet(parquet_path)
        except Exception as exc:
            return {
                "success": False,
                "output": "",
                "result": None,
                "plots": [],
                "error": f"Failed to load parquet file '{parquet_path}': {exc}",
            }

        # ── Auto-coerce object columns to numeric or datetime ───────────────
        # ClickHouse can return Date/DateTime as strings or Decimal as objects.
        # This prevents common TypeErrors in agent-written Python code.
        # Array columns (list values) are left as-is — they are already usable in Python.
        for col in list(df.select_dtypes(include="object").columns):
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            # Skip Array columns (list values) — no coercion needed
            if isinstance(non_null.iloc[0], list):
                continue
            # Try numeric conversion first
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() / len(non_null) > 0.8:
                df[col] = converted
                continue
            # Try datetime conversion
            try:
                dt_converted = pd.to_datetime(df[col], errors="coerce", format="mixed")
                if dt_converted.notna().sum() / len(non_null) > 0.8:
                    df[col] = dt_converted
            except Exception:
                pass

        # Close any stray figures from previous runs
        plt.close("all")

        # ── Prepare execution namespace ────────────────────────────────────
        local_vars = {
            "df": df,
            "pd": pd,
            "np": np,
            "plt": plt,
            "sns": sns,
            "result": None,  # agent sets this for final text output
            # Type map with Array detection: {"col": "int64"} or {"col": "Array(object)"}
            "df_info": {
                col: (
                    "Array(object)"
                    if str(dtype) == "object"
                    and len(df[col].dropna()) > 0
                    and isinstance(df[col].dropna().iloc[0], list)
                    else str(dtype)
                )
                for col, dtype in df.dtypes.items()
            },
        }

        stdout_capture = io.StringIO()
        plots: list[str] = []

        # Libraries are placed in globals so they remain accessible inside
        # user-defined lambdas called back by pandas/numpy (e.g. .apply()).
        # local_vars (df, result, df_info) take precedence over globals in exec.
        sandbox_globals = {
            "__builtins__": __builtins__,
            "pd": pd,
            "np": np,
            "plt": plt,
            "sns": sns,
        }

        try:
            # ── Execute code with captured stdout ──────────────────────────
            with contextlib.redirect_stdout(stdout_capture):
                exec(code, sandbox_globals, local_vars)  # noqa: S102

            # ── Capture all matplotlib figures ─────────────────────────────
            for fig_num in plt.get_fignums():
                fig = plt.figure(fig_num)
                buf = io.BytesIO()
                fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
                buf.seek(0)
                b64 = base64.b64encode(buf.read()).decode("utf-8")
                plots.append(f"data:image/png;base64,{b64}")
                buf.close()

            # ── Extract `result` variable ──────────────────────────────────
            result_value = local_vars.get("result")
            if isinstance(result_value, pd.DataFrame):
                # Convert DataFrame to Markdown table
                result_value = result_value.to_markdown(index=False)
            elif result_value is not None:
                result_value = str(result_value)

            return {
                "success": True,
                "output": stdout_capture.getvalue(),
                "result": result_value,
                "plots": plots,
                "error": None,
            }

        except Exception as exc:
            # Keep only the last 1 500 chars of the traceback — the tail contains
            # the actual error line and is sufficient for the LLM to self-correct.
            full_tb = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            error_text = full_tb[-1500:] if len(full_tb) > 1500 else full_tb
            return {
                "success": False,
                "output": stdout_capture.getvalue(),
                "result": None,
                "plots": plots,  # return any plots captured before the error
                "error": error_text,
            }

        finally:
            # Always clean up figures and local vars
            plt.close("all")
            local_vars.clear()
