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
        for col in list(df.select_dtypes(include="object").columns):
            non_null = df[col].dropna()
            if len(non_null) == 0:
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

        # ── Convert numpy-array columns to Python lists ────────────────────
        # ClickHouse Array(X) columns survive parquet round-trip as numpy
        # object-arrays. Standard pandas ops (to_markdown, tabulate) raise
        # "truth value of an array is ambiguous" on them. Converting to plain
        # Python lists makes the data fully compatible with all pandas/tabulate
        # operations while preserving the values.
        array_cols: list[str] = []
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            if isinstance(non_null.iloc[0], np.ndarray):
                df[col] = df[col].apply(
                    lambda v: v.tolist() if isinstance(v, np.ndarray) else v
                )
                array_cols.append(col)

        # Close any stray figures from previous runs
        plt.close("all")

        # ── Prepare execution namespace ────────────────────────────────────
        # df_info shows dtype for regular cols and "Array" for list cols so
        # the agent knows to use .explode() / .apply(len) instead of to_markdown.
        df_info = {col: str(dtype) for col, dtype in df.dtypes.items()}
        for col in array_cols:
            df_info[col] = "Array"

        local_vars = {
            "df": df,
            "pd": pd,
            "np": np,
            "plt": plt,
            "sns": sns,
            "result": None,  # agent sets this for final text output
            "df_info": df_info,
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

            # ── Truncate stdout to avoid flooding LLM context ──────────────
            _MAX_OUTPUT = 3000
            raw_output = stdout_capture.getvalue()
            if len(raw_output) > _MAX_OUTPUT:
                half = _MAX_OUTPUT // 2
                raw_output = (
                    raw_output[:half]
                    + f"\n… [stdout truncated: {len(raw_output)} chars total] …\n"
                    + raw_output[-half:]
                )

            return {
                "success": True,
                "output": raw_output,
                "result": result_value,
                "plots": plots,
                "error": None,
            }

        except Exception as exc:
            # Keep only the last 1 500 chars of the traceback — the tail contains
            # the actual error line and is sufficient for the LLM to self-correct.
            full_tb = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            error_text = full_tb[-1500:] if len(full_tb) > 1500 else full_tb
            raw_output = stdout_capture.getvalue()
            return {
                "success": False,
                "output": raw_output[:3000] if len(raw_output) > 3000 else raw_output,
                "result": None,
                "plots": plots,  # return any plots captured before the error
                "error": error_text,
            }

        finally:
            # Always clean up figures and local vars
            plt.close("all")
            local_vars.clear()
