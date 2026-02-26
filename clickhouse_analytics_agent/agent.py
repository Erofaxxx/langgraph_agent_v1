"""
LangGraph-based ClickHouse Analytics Agent.

Architecture:
  - LLM  : Claude Sonnet 4.6 via OpenRouter (ChatOpenAI adapter)
  - Graph : LangGraph create_react_agent (tool-calling loop)
  - Memory: SqliteSaver checkpointer — persists full conversation per session_id
  - Tools : list_tables, clickhouse_query, python_analysis

Session isolation:
  Every API request carries a session_id (= LangGraph thread_id).
  SqliteSaver stores the message state keyed by thread_id.
  Multiple concurrent sessions do NOT interfere with each other.
"""

import json
import time
import sqlite3
from typing import Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.prebuilt import create_react_agent

from config import (
    DB_PATH,
    MAX_AGENT_ITERATIONS,
    MAX_TOKENS,
    MODEL,
    OPENROUTER_API_KEY,
    TEMP_DIR,
    TEMP_FILE_TTL_SECONDS,
)
from tools import TOOLS

# ─── System Prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """Ты — опытный аналитик рекламных данных. Ты работаешь с базой данных ClickHouse, которая содержит данные о рекламных кампаниях, визитах на сайт, витринах и маркетинговых метриках. Ты помогаешь маркетологам отвечать на вопросы, строить отчёты и считать ключевые показатели.

## Рабочий процесс (выполняй строго по порядку):

### 1. Понять запрос
Определи: какие данные нужны, нужна ли визуализация, нужна ли таблица, какие метрики считать.

### 2. Изучить схему (ТОЛЬКО при первом запросе в сессии)
Если структура таблиц ещё неизвестна — вызови `list_tables`.
Если она уже известна из истории диалога — ПРОПУСТИ этот шаг.

### 3. Выгрузить данные из ClickHouse
Вызови `clickhouse_query` с оптимальным SQL:
- Агрегируй данные прямо в SQL (SUM, COUNT, AVG, GROUP BY) — ClickHouse очень быстр
- Фильтруй в WHERE — не выгружай лишнее
- LIMIT: обычно 1000–10000; до 50000 для больших выборок
- Функции: toStartOfMonth(), toYear(), toDayOfWeek(), arrayJoin() и т.д.
- Сохрани `parquet_path` из ответа для python_analysis

### 4. Проанализировать данные в Python
Вызови `python_analysis` для расчётов и визуализации:
- Строй графики (bar, line, pie, scatter, heatmap)
- Формируй Markdown-таблицы
- Считай метрики (CTR, CPC, CPM, ROAS, CR, CPA)
- Устанавливай переменную `result` с итоговым Markdown-выводом

### 5. Сформировать финальный ответ
Дай конкретный аналитический вывод с цифрами, сравнениями и рекомендациями.

---

## Правила Python-кода:
1. `df` уже загружен — НЕ вызывай pd.read_parquet()
2. ВСЕГДА устанавливай `result` (Markdown строка с итогом)
3. Используй print() для логирования шагов: print("📊 Шаг 1: ...")
4. Подписывай графики на РУССКОМ: plt.title(), plt.xlabel(), plt.ylabel()
5. Форматируй числа: f"{value:,.0f}" (целые), f"{value:,.2f}" (дробные)
6. Обрабатывай пропуски: df.dropna() или df.fillna(0)
7. Для каждого графика — plt.tight_layout() перед следующим

## Рекламные метрики:
- **CTR** = клики / показы × 100%
- **CPC** = расход / клики
- **CPM** = расход / показы × 1000
- **CPA** = расход / конверсии
- **ROAS** = доход / расход × 100%
- **CR** = конверсии / клики × 100%

## Стиль ответа:
- Markdown: заголовки ##/###, таблицы, списки
- Эмодзи для структурирования: 📊 📈 📉 💰 ✅ ⚠️
- Числа с разделителями тысяч
- Язык — русский
- Конкретика: цифры, динамика, сравнение с нормой
"""


class AnalyticsAgent:
    """
    Wraps LangGraph ReAct agent with:
      - Claude Sonnet 4.6 via OpenRouter
      - SqliteSaver for session memory
      - Helper methods to extract plots and tool-call logs from agent output
    """

    def __init__(self) -> None:
        if not OPENROUTER_API_KEY:
            raise ValueError(
                "OPENROUTER_API_KEY is not set in .env. "
                "Get your key at https://openrouter.ai"
            )

        # ── LLM via OpenRouter ────────────────────────────────────────────
        self.llm = ChatOpenAI(
            model=MODEL,
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            max_tokens=MAX_TOKENS,
            default_headers={
                "HTTP-Referer": "https://server.asktab.ru",
                "X-Title": "ClickHouse Analytics Agent",
            },
        )

        # ── SqliteSaver checkpointer ──────────────────────────────────────
        # Keeps conversation state per thread_id (= session_id).
        # Thread-safe for concurrent requests.
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.memory = SqliteSaver(conn)


        # ── LangGraph ReAct agent ─────────────────────────────────────────
        # state_modifier prepends the system prompt before every LLM call
        # (not stored in the checkpoint — safe and clean).
        self.graph = create_react_agent(
            model=self.llm,
            tools=TOOLS,
            prompt=SystemMessage(content=SYSTEM_PROMPT),
            checkpointer=self.memory,
        )

        print(f"✅ AnalyticsAgent ready | model: {MODEL} | db: {DB_PATH}")

    # ─── Public API ───────────────────────────────────────────────────────────

    def analyze(self, user_query: str, session_id: str) -> dict:
        """
        Process a user analytics query for a given session.

        Args:
            user_query: The user's question or request.
            session_id: Unique session identifier (= LangGraph thread_id).
                        Reuse the same session_id across requests to maintain context.

        Returns:
            {
              "success":    bool,
              "session_id": str,
              "text_output": str,         # Final Markdown text from the agent
              "plots":      list[str],    # base64 PNG data URIs from python_analysis
              "tool_calls": list[dict],   # Log of tool invocations
              "error":      str | None,
            }
        """
        config = {"configurable": {"thread_id": session_id}}

        try:
            # LangGraph invoke — sends only the NEW message;
            # history is loaded automatically from SqliteSaver by thread_id.
            result = self.graph.invoke(
                {"messages": [HumanMessage(content=user_query)]},
                config=config,
            )

            messages: list = result.get("messages", [])

            text_output = self._extract_final_text(messages)
            plots = self._extract_plots(messages)
            tool_calls = self._extract_tool_calls(messages)

            return {
                "success": True,
                "session_id": session_id,
                "text_output": text_output,
                "plots": plots,
                "tool_calls": tool_calls,
                "error": None,
            }

        except Exception as exc:
            import traceback as tb
            return {
                "success": False,
                "session_id": session_id,
                "text_output": "",
                "plots": [],
                "tool_calls": [],
                "error": str(exc),
                "traceback": tb.format_exc(),
            }

    def get_session_info(self, session_id: str) -> dict:
        """Return basic metadata about a session."""
        try:
            config = {"configurable": {"thread_id": session_id}}
            state = self.graph.get_state(config)
            msgs = state.values.get("messages", []) if state and state.values else []
            # Count only user-visible exchanges (HumanMessage + AIMessage pairs)
            user_msgs = sum(1 for m in msgs if isinstance(m, HumanMessage))
            return {
                "session_id": session_id,
                "total_messages": len(msgs),
                "user_turns": user_msgs,
                "has_history": user_msgs > 0,
            }
        except Exception:
            return {
                "session_id": session_id,
                "total_messages": 0,
                "user_turns": 0,
                "has_history": False,
            }

    def cleanup_temp_files(self) -> int:
        """Delete Parquet files older than TEMP_FILE_TTL_SECONDS. Returns count deleted."""
        cutoff = time.time() - TEMP_FILE_TTL_SECONDS
        deleted = 0
        for f in TEMP_DIR.glob("*.parquet"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1
            except OSError:
                pass
        if deleted:
            print(f"🗑️  Deleted {deleted} expired parquet file(s)")
        return deleted

    # ─── Private helpers ──────────────────────────────────────────────────────

    def _extract_final_text(self, messages: list) -> str:
        """Return content of the last AIMessage that has non-empty text."""
        for msg in reversed(messages):
            if not isinstance(msg, AIMessage):
                continue
            content = msg.content
            if isinstance(content, str) and content.strip():
                return content
            # Some models return list of content blocks
            if isinstance(content, list):
                parts = [
                    block["text"]
                    for block in content
                    if isinstance(block, dict) and block.get("type") == "text"
                ]
                text = "\n".join(parts).strip()
                if text:
                    return text
        return ""

    def _extract_plots(self, messages: list) -> list[str]:
        """
        Extract base64 PNG plots from python_analysis ToolMessages
        that belong to the CURRENT agent run (after the last HumanMessage).
        """
        # Find index of the most recently added HumanMessage
        last_human_idx = -1
        for i, msg in enumerate(messages):
            if isinstance(msg, HumanMessage):
                last_human_idx = i

        if last_human_idx < 0:
            return []

        plots: list[str] = []
        for msg in messages[last_human_idx:]:
            if not isinstance(msg, ToolMessage):
                continue
            tool_name = getattr(msg, "name", "") or ""
            if tool_name != "python_analysis":
                continue
            try:
                data = json.loads(msg.content)
                if isinstance(data, dict) and data.get("plots"):
                    plots.extend(data["plots"])
            except (json.JSONDecodeError, AttributeError):
                pass

        return plots

    def _extract_tool_calls(self, messages: list) -> list[dict]:
        """
        Extract a compact log of tool calls made during the current run.
        """
        last_human_idx = -1
        for i, msg in enumerate(messages):
            if isinstance(msg, HumanMessage):
                last_human_idx = i

        if last_human_idx < 0:
            return []

        tool_calls: list[dict] = []
        for msg in messages[last_human_idx:]:
            if not isinstance(msg, AIMessage):
                continue
            for tc in getattr(msg, "tool_calls", []):
                name = tc.get("name", "")
                args = tc.get("args", {})
                # Truncate large args for the log
                compact_args = {
                    k: (v[:300] + "…" if isinstance(v, str) and len(v) > 300 else v)
                    for k, v in args.items()
                }
                tool_calls.append({"tool": name, "input": compact_args})

        return tool_calls


# ─── Global singleton ─────────────────────────────────────────────────────────
_agent: Optional[AnalyticsAgent] = None


def get_agent() -> AnalyticsAgent:
    """Return (or create) the global AnalyticsAgent instance."""
    global _agent
    if _agent is None:
        _agent = AnalyticsAgent()
    return _agent
