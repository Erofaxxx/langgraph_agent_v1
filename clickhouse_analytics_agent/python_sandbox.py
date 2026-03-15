"""
Python code execution sandbox for the Analytics Agent.

Loads data from a Parquet file into a pandas DataFrame and executes
user-provided code in a controlled namespace. Captures:
  - stdout (print statements)
  - matplotlib/seaborn figures → base64 PNG strings
  - `result` variable → final text/table output
"""

import base64
import builtins as _builtins_module
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


class _PlotProxy:
    """
    Proxy for matplotlib.pyplot injected into the agent's execution namespace.

    Makes plt.close() and plt.savefig() silent no-ops so that agent code
    cannot accidentally destroy figures before the sandbox captures them.
    All other plt attributes/methods delegate transparently to the real plt.
    This works even when the visualization skill is NOT loaded.
    """
    def __getattr__(self, name: str):
        if name in ("close", "savefig"):
            return lambda *a, **kw: None  # no-op — figure stays open for capture
        return getattr(plt, name)


_plt_proxy = _PlotProxy()


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

        # ── Snapshot existing figure numbers before exec ───────────────────
        # matplotlib is a global singleton shared across all parallel calls.
        # We record which figures exist BEFORE our exec, then after exec we
        # capture and close only the NEW figures created by THIS call.
        # This prevents parallel python_analysis calls from interfering with
        # each other's figures via plt.close("all").
        _before_fignums: set[int] = set(plt.get_fignums())

        # ── Prepare execution namespace ────────────────────────────────────
        # df_info shows dtype for regular cols and "Array" for list cols so
        # the agent knows to use .explode() / .apply(len) instead of to_markdown.
        df_info = {col: str(dtype) for col, dtype in df.dtypes.items()}
        for col in array_cols:
            df_info[col] = "Array"

        # ── Thread-safe stdout capture ─────────────────────────────────────
        # contextlib.redirect_stdout is NOT thread-safe: it sets sys.stdout
        # globally, so two parallel calls overwrite each other's capture buffer.
        # Instead, we inject a custom print() into the execution builtins that
        # writes directly to our per-call StringIO — fully isolated.
        stdout_capture = io.StringIO()

        def _captured_print(*args, sep=" ", end="\n", file=None, flush=False):
            _builtins_module.print(
                *args, sep=sep, end=end,
                file=stdout_capture if file is None else file,
                flush=flush,
            )

        _patched_builtins = {**vars(_builtins_module), "print": _captured_print}

        plots: list[str] = []

        # Single namespace for exec: all names — pre-set vars, libraries, and
        # user-defined variables — live in one dict. This is essential because
        # when exec is called with separate globals/locals, functions and lambdas
        # defined inside the exec'd code use globals as their __globals__, making
        # any top-level local variable invisible inside those functions/lambdas.
        # One dict eliminates that split entirely.
        sandbox_globals = {
            "__builtins__": _patched_builtins,
            "pd": pd,
            "np": np,
            "plt": _plt_proxy,   # proxy: close()/savefig() are no-ops
            "sns": sns,
            "df": df,
            "result": None,  # agent sets this for final text output
            "df_info": df_info,
        }

        try:
            # ── Execute code ────────────────────────────────────────────────
            exec(code, sandbox_globals)  # noqa: S102

            # ── Capture only figures created by THIS call ──────────────────
            _my_fignums: set[int] = set(plt.get_fignums()) - _before_fignums
            for fig_num in sorted(_my_fignums):
                fig = plt.figure(fig_num)
                buf = io.BytesIO()
                fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
                buf.seek(0)
                b64 = base64.b64encode(buf.read()).decode("utf-8")
                plots.append(f"data:image/png;base64,{b64}")
                buf.close()

            # ── Extract `result` variable ──────────────────────────────────
            result_value = sandbox_globals.get("result")
            if isinstance(result_value, pd.DataFrame):
                # Convert DataFrame to Markdown table
                result_value = result_value.to_markdown(index=False)
            elif result_value is not None:
                result_value = str(result_value)

            # ── Truncate stdout to avoid flooding LLM context ──────────────
            # 8 000 chars ≈ 100-150 rows of tabular data — enough to understand
            # structure and values without causing context explosion.
            # head+tail strategy keeps both schema rows and tail rows visible.
            _MAX_OUTPUT = 8000
            raw_output = stdout_capture.getvalue()
            if len(raw_output) > _MAX_OUTPUT:
                half = _MAX_OUTPUT // 2
                raw_output = (
                    raw_output[:half]
                    + f"\n… [stdout truncated, showing first+last {half} chars"
                    f" of {len(raw_output)} total — data in parquet is complete] …\n"
                    + raw_output[-half:]
                )

            # ── Truncate result variable ────────────────────────────────────
            # result is shown to the user; keep it readable but bounded.
            _MAX_RESULT = 12000
            if result_value and len(result_value) > _MAX_RESULT:
                half_r = _MAX_RESULT // 2
                result_value = (
                    result_value[:half_r]
                    + f"\n… [result truncated: {len(result_value)} chars total] …\n"
                    + result_value[-half_r:]
                )

            return {
                "success": True,
                "output": raw_output,
                "result": result_value,
                "plots": plots,
                "error": None,
            }

        except Exception as exc:
            # Keep only the last 2 000 chars of the traceback — the tail contains
            # the actual error line and is sufficient for the LLM to self-correct.
            full_tb = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            error_text = full_tb[-2000:] if len(full_tb) > 2000 else full_tb
            raw_output = stdout_capture.getvalue()
            return {
                "success": False,
                "output": raw_output[:8000] if len(raw_output) > 8000 else raw_output,
                "result": None,
                "plots": plots,  # return any plots captured before the error
                "error": error_text,
            }

        finally:
            # Close only figures created by THIS call, not figures from
            # other parallel calls that may still be running.
            _to_close = set(plt.get_fignums()) - _before_fignums
            for fig_num in _to_close:
                try:
                    plt.close(fig_num)
                except Exception:
                    pass
            sandbox_globals.clear()
