"""
LangGraph-based ClickHouse Analytics Agent.

Architecture:
  - LLM  : Claude Sonnet 4.6 via OpenRouter (ChatOpenAI adapter)
  - Graph : LangGraph create_react_agent (tool-calling loop)
  - Memory: SqliteSaver checkpointer — persists full conversation per session_id
  - Tools : list_tables (fallback), clickhouse_query, python_analysis

Session isolation:
  Every API request carries a session_id (= LangGraph thread_id).
  SqliteSaver stores the message state keyed by thread_id.
  Multiple concurrent sessions do NOT interfere with each other.

Context optimisations (in _build_messages, a per-instance closure):
  1. Static schema embedded in system prompt at startup — no list_tables round-trip.
  2. Sliding window: only last MAX_HISTORY_TURNS human turns kept in context.
  3. ToolMessage compression for previous turns (keeps metadata, drops heavy payload).
  4. AIMessage compression: strips reasoning text from old turns, keeps tool_calls.
"""

import json
import time
import sqlite3
from copy import copy
from typing import Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.prebuilt import create_react_agent

from config import (
    DB_PATH,
    MAX_AGENT_ITERATIONS,
    MAX_HISTORY_TURNS,
    MAX_TOKENS,
    MODEL,
    MODEL_PROVIDER,
    OPENROUTER_API_KEY,
    TEMP_DIR,
    TEMP_FILE_TTL_SECONDS,
)
from tools import TOOLS

# ─── Context compression helpers ──────────────────────────────────────────────

def _compress_tool_message(msg: ToolMessage) -> ToolMessage:
    """
    Replace a ToolMessage's content with a compact version.

    Called only for ToolMessages from PREVIOUS turns so the LLM receives
    minimal but sufficient information about past tool results.

    Compression strategy per tool:
      list_tables    → keep table+column names, drop types   (~60–70% smaller)
      clickhouse_query → keep metadata only, drop preview rows (~40–50% smaller)
      python_analysis  → keep result summary only, drop stdout  (~50–80% smaller)
    """
    tool_name = getattr(msg, "name", "") or ""
    content = msg.content

    try:
        if tool_name == "list_tables":
            data = json.loads(content)
            # Normalise to plain column-name lists.
            # Handles both old format (columns: [{"name": ..., "type": ...}])
            # and new format (columns: ["col1", "col2", ...]) for checkpoint compat.
            compact = []
            for t in data:
                cols = t.get("columns", [])
                if cols and isinstance(cols[0], dict):
                    col_names = [c["name"] for c in cols]
                else:
                    col_names = cols
                compact.append({"table": t["table"], "columns": col_names})
            new_content = json.dumps(compact, ensure_ascii=False)

        elif tool_name == "clickhouse_query":
            data = json.loads(content)
            # Drop preview_first_5_rows and dtypes — already analysed in this turn
            new_content = json.dumps(
                {
                    "success": data.get("success"),
                    "row_count": data.get("row_count"),
                    "columns": data.get("columns"),
                    "parquet_path": data.get("parquet_path"),
                },
                ensure_ascii=False,
            )

        elif tool_name == "python_analysis":
            data = json.loads(content)
            # Drop stdout logs — keep only the final result (capped at 500 chars)
            result_text = data.get("result") or ""
            new_content = json.dumps(
                {
                    "success": data.get("success"),
                    "result": result_text[:500] + ("…" if len(result_text) > 500 else ""),
                },
                ensure_ascii=False,
            )

        else:
            return msg

    except Exception:
        # If parsing fails, return the original message unchanged
        return msg

    try:
        return msg.model_copy(update={"content": new_content})
    except Exception:
        new_msg = copy(msg)
        new_msg.content = new_content
        return new_msg


def _build_schema_block(tables: list[dict]) -> str:
    """
    Format a list of {table, columns} dicts into a compact schema section
    for embedding in the system prompt.

    Example output:
      **orders**: id, user_id, date, amount, status
      **sessions**: id, date, utm_source, utm_medium, revenue
    """
    lines = []
    for t in tables:
        cols = t.get("columns", [])
        # columns may be a list of strings or list of dicts (legacy)
        if cols and isinstance(cols[0], dict):
            col_names = [c["name"] for c in cols]
        else:
            col_names = [str(c) for c in cols]
        lines.append(f"**{t['table']}**: {', '.join(col_names)}")
    return "\n".join(lines)


# ─── System Prompt template ────────────────────────────────────────────────────
# {schema_section} is filled at agent startup with the live schema or a fallback.

_SYSTEM_PROMPT_TEMPLATE = """Ты — лучший в мире аналитик рекламных данных. Работаешь с ClickHouse-базой компании.
Твоя задача — отвечать на вопросы маркетолога по данным: трафик, покупки, кампании, поведение клиентов.

Стиль работы: ты находишься внутри рабочего процесса — маркетолог работает с данными каждый день, задаёт много вопросов подряд, возвращается к предыдущим темам, уточняет. Ты часть этого потока, не разовый отчёт. Отвечай коротко и по делу — как коллега, который уже в контексте.

## Схема базы данных

{schema_section}

### Принцип работы
Ты ведёшь расследование, а не отвечаешь на изолированные вопросы. Держи нить:
* Помни что уже выяснили в этой сессии — не повторяй, опирайся
* Если данные противоречат здравому смыслу — скажи первым, не жди вопроса
* После ответа видь следующий логичный шаг — предложи его одной строкой
* Не принимай данные за истину без проверки: аномалия, малая выборка, методология фильтрации — всё под сомнением пока не объяснено

## Рабочий процесс (выполняй строго по порядку):

### 1. Понять запрос
Определи тип — от этого зависит всё остальное:

- Факт ("сколько", "покажи", "топ") → одна цифра или таблица, без выводов
- Анализ ("почему", "сравни", "есть ли разница") → данные + 1–2 инсайта
- Интерпретация ("это норма?", "хорошо или плохо?") → одна витрина + маркетинговая логика, без тяжёлых JOIN-ов
- Drill-down ("разбери", "детализируй") → полная детализация уместна
- Уточнение к предыдущему → сначала проверь, можно ли ответить из уже выгруженных данных; в базу — только если нет

### 2. Схема таблиц

Схема базы данных уже предоставлена в начале этого промпта — НЕ вызывай `list_tables`.
Используй `list_tables` только если схема кажется неполной или таблица не найдена.

### 3. Выгрузить данные из ClickHouse
Вызови `clickhouse_query` с оптимальным SQL:
* Агрегируй данные прямо в SQL (SUM, COUNT, AVG, GROUP BY) — ClickHouse очень быстр
* Фильтруй в WHERE — не выгружай лишнее
* LIMIT: обычно 1000–10000; до 50000 для больших выборок
* Функции: toStartOfMonth(), toYear(), toDayOfWeek(), arrayJoin() и т.д.
* Сохрани `parquet_path` из ответа для python_analysis
* Начинай с одной витрины. JOIN — только если без него принципиально не решить
* При временной фильтрации — указывай в ответе по какому полю фильтруешь

### 4. Проанализировать данные в Python
Вызови `python_analysis` для расчётов:
* Формируй Markdown-таблицы
* Считай метрики (CTR, CPC, CPM, ROAS, CR, CPA)
* Устанавливай переменную `result` с итоговым Markdown-выводом
* График — только если вопрос про динамику, тренд или сравнение нескольких сущностей. На факты и разовые цифры — не нужен

При анализе:
* Если n < 5 — помечай ⚠️, выводов не строить
* При ранжировании по среднему чеку или CR — всегда показывай n (количество заказов/сессий), иначе топ статистически бессмысленен
* Аномалии — исследуй, не игнорируй. Одна строка с 90% выручки — это сигнал, не норма
* Если данных для вывода недостаточно — скажи это прямо и предложи следующий шаг

### 5. Сформировать финальный ответ
Формат зависит от типа запроса (см. шаг 1):
* Факт → прямой ответ первым предложением. Таблица если нужна. Всё.
* Анализ → данные + максимум 2 инсайта. Без раздела "Ключевые выводы" — если инсайт уже в таблице, не повторяй
* Интерпретация → вывод на основе данных + маркетинговая логика. Без домыслов о бизнесе
* Drill-down → полная детализация, гипотезы, объяснение аномалий

Рекомендации — только при трёх условиях одновременно:
1. Данные для неё есть в выгрузке
2. Канал/инструмент виден в данных (не предлагать то, чего нет)
3. Для масштабирования — есть CR или spend по этой сущности

Если данных недостаточно → предложи следующий шаг: "Проверить CR этой кампании?"

Проактивность: если в данных виден нетривиальный паттерн — назови его, даже если не спросили. Один инсайт сверх вопроса — норма. Два и больше — уже балласт.

Каждое слово в выводе должно нести смысл. Никаких итоговых блоков с эмодзи, повторов, обобщений ради обобщений.

## Правила Python-кода:
1. `df` уже загружен — НЕ вызывай pd.read_parquet()
2. ВСЕГДА устанавливай `result` (Markdown строка с итогом)
3. Используй print() для логирования шагов: print("📊 Шаг 1: ...")
4. Подписывай графики на РУССКОМ: plt.title(), plt.xlabel(), plt.ylabel()
5. Форматируй числа: f"{{value:,.0f}}" (целые), f"{{value:,.2f}}" (дробные)
6. Обрабатывай пропуски: df.dropna() или df.fillna(0)
7. Для каждого графика — plt.tight_layout() перед следующим
8. График строй ТОЛЬКО если он явно нужен по типу вопроса (см. шаг 4) — не по умолчанию

## Рекламные метрики
Формулы не нужно воспроизводить в каждом ответе. Но:
* Если считаешь нестандартную метрику или производную — покажи формулу один раз
* Если вводишь аббревиатуру которую маркетолог мог не знать — расшифруй при первом упоминании
* Если данных для метрики нет (нет расхода → нет CPC/CPA/ROAS) — скажи прямо, не додумывай

## Стиль ответа
* Markdown: заголовки ##/###, таблицы, жирный для ключевых цифр
* Эмодзи — только ⚠️ для предупреждений. Больше нигде
* Числа с разделителями тысяч: 1 234 567
* Язык — русский
* Конкретика: цифры, динамика, сравнение — без воды
"""


# ─── LLM factory ──────────────────────────────────────────────────────────────

_OPENROUTER_HEADERS = {
    "HTTP-Referer": "https://server.asktab.ru",
    "X-Title": "ClickHouse Analytics Agent",
}


def _create_llm():
    """
    Return the appropriate LangChain LLM based on MODEL_PROVIDER.

    anthropic → ChatAnthropic via OpenRouter.
                Supports Anthropic content-block format, so cache_control
                markers in SystemMessage are forwarded to the API correctly.

    deepseek  → ChatOpenAI via OpenRouter (current behaviour, unchanged).
                No prompt caching — DeepSeek doesn't offer it.
    """
    if MODEL_PROVIDER == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=MODEL,
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            max_tokens=MAX_TOKENS,
            default_headers=_OPENROUTER_HEADERS,
        )
    else:
        # deepseek (and any future OpenAI-compatible provider)
        return ChatOpenAI(
            model=MODEL,
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            max_tokens=MAX_TOKENS,
            default_headers=_OPENROUTER_HEADERS,
        )


class AnalyticsAgent:
    """
    Wraps LangGraph ReAct agent with:
      - Claude Sonnet 4.6 (prompt caching) or DeepSeek, both via OpenRouter
      - SqliteSaver for session memory
      - Dynamic system prompt with embedded DB schema (fetched once at startup)
      - Per-request context optimisation: sliding window + AIMessage/ToolMessage compression
    """

    def __init__(self) -> None:
        if not OPENROUTER_API_KEY:
            raise ValueError(
                "OPENROUTER_API_KEY is not set in .env. "
                "Get your key at https://openrouter.ai"
            )

        # ── LLM via OpenRouter ────────────────────────────────────────────
        self.llm = _create_llm()

        # ── SqliteSaver checkpointer ──────────────────────────────────────
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.memory = SqliteSaver(conn)

        # ── Embed static schema into system prompt ────────────────────────
        # Tables don't change at runtime, so fetching once at startup is safe.
        # This eliminates the list_tables round-trip on every new session.
        system_prompt = self._build_system_prompt()

        # ── Message builder closure (captures system_prompt + provider) ───
        # Passed to create_react_agent as `prompt=` so it runs before every
        # LLM call.  Applies three optimisations:
        #   1. Sliding window — keep only last MAX_HISTORY_TURNS human turns.
        #   2. AIMessage compression — strip reasoning text from old turns
        #      while preserving tool_calls (required for graph integrity).
        #   3. ToolMessage compression — keep metadata, drop heavy payloads.
        #
        # SystemMessage format differs by provider:
        #   anthropic → content block list with cache_control (prompt caching)
        #   deepseek  → plain string (OpenAI-compatible, no caching)
        is_anthropic = MODEL_PROVIDER == "anthropic"

        def _build_messages(state: dict) -> list:
            messages = state.get("messages", [])

            # ── 1. Sliding window ──────────────────────────────────────────
            human_indices = [
                i for i, m in enumerate(messages) if isinstance(m, HumanMessage)
            ]
            if len(human_indices) > MAX_HISTORY_TURNS:
                cutoff = human_indices[-MAX_HISTORY_TURNS]
                messages = messages[cutoff:]

            # ── Locate current-turn boundary ──────────────────────────────
            # current_turn_start = index of the last HumanMessage (in the
            # possibly-sliced list).
            current_turn_start = 0
            for i, msg in enumerate(messages):
                if isinstance(msg, HumanMessage):
                    current_turn_start = i

            # Within the current turn: the last AIMessage that issued tool
            # calls.  ToolMessages before it have already been consumed and
            # can be safely compressed.
            last_tool_calling_ai_idx = -1
            for i in range(len(messages) - 1, current_turn_start - 1, -1):
                m = messages[i]
                if isinstance(m, AIMessage) and getattr(m, "tool_calls", None):
                    last_tool_calling_ai_idx = i
                    break

            # ── 2 & 3. Compress old messages ──────────────────────────────
            compressed: list = []
            for i, msg in enumerate(messages):
                if isinstance(msg, ToolMessage):
                    if i < current_turn_start or last_tool_calling_ai_idx > i:
                        compressed.append(_compress_tool_message(msg))
                    else:
                        compressed.append(msg)

                elif isinstance(msg, AIMessage) and i < current_turn_start:
                    # Previous turns: keep tool_calls (graph needs them to
                    # pair with ToolMessages), but drop the reasoning text
                    # which can be thousands of tokens and is no longer useful.
                    if getattr(msg, "tool_calls", None):
                        try:
                            compressed.append(msg.model_copy(update={"content": ""}))
                        except Exception:
                            new_msg = copy(msg)
                            new_msg.content = ""
                            compressed.append(new_msg)
                    else:
                        # Final answer to user — keep as conversation context
                        compressed.append(msg)

                else:
                    compressed.append(msg)

            # For Anthropic: wrap system prompt in a content block and mark it
            # as a cache breakpoint.  The entire system prompt (including the
            # embedded schema) is static across all calls, so it's the ideal
            # cache candidate — repeated calls in the same session pay only
            # for cache-read tokens (10× cheaper than input tokens).
            #
            # For DeepSeek (and other OpenAI-compatible providers): plain
            # string — cache_control is not supported.
            if is_anthropic:
                system_msg = SystemMessage(content=[
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ])
            else:
                system_msg = SystemMessage(content=system_prompt)

            return [system_msg] + compressed

        # ── LangGraph ReAct agent ─────────────────────────────────────────
        self.graph = create_react_agent(
            model=self.llm,
            tools=TOOLS,
            prompt=_build_messages,
            checkpointer=self.memory,
        )

        caching_info = "prompt caching ON" if is_anthropic else "no prompt caching"
        print(f"✅ AnalyticsAgent ready | provider: {MODEL_PROVIDER} | model: {MODEL} | {caching_info} | db: {DB_PATH}")

    # ─── System prompt builder ─────────────────────────────────────────────────

    def _build_system_prompt(self) -> str:
        """
        Fetch the DB schema and embed it into the system prompt.
        Falls back to a generic notice if ClickHouse is unreachable.
        """
        try:
            from tools import _get_ch_client
            tables = _get_ch_client().list_tables()
            schema_block = _build_schema_block(tables)
            schema_section = (
                "Схема таблиц (статичная, загружена при старте агента):\n\n"
                + schema_block
            )
            print(f"✅ Schema loaded: {len(tables)} table(s) embedded in system prompt")
        except Exception as exc:
            schema_section = (
                "Схема недоступна при старте. "
                "Используй инструмент `list_tables` чтобы получить список таблиц."
            )
            print(f"⚠️  Could not fetch schema at startup: {exc}")

        return _SYSTEM_PROMPT_TEMPLATE.format(schema_section=schema_section)

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
            if (getattr(msg, "name", "") or "") != "python_analysis":
                continue
            artifact = getattr(msg, "artifact", None)
            if isinstance(artifact, list):
                plots.extend(artifact)

        return plots

    def _extract_tool_calls(self, messages: list) -> list[dict]:
        """Extract a compact log of tool calls made during the current run."""
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
