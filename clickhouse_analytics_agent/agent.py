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
  3. Turn summarisation: each previous turn's internal tool-call chain (AIMessage+
     tool_calls + ToolMessages) is replaced by a compact SQL/row-count AIMessage.
     The final agent answer (shown to user) is preserved verbatim.
     Tool chain: ~830 tokens → ~50 tokens; final answer kept as-is.
  4. Intra-turn ToolMessage compression: within the current request, once
     python_analysis has been called, preceding clickhouse_query ToolMessages have
     their dtypes + preview_first_5_rows stripped.  Also drops stdout from earlier
     python_analysis runs in a retry scenario.
  5. Prompt caching: system prompt + last history message marked with cache_control
     (Anthropic via OpenRouter) — ~68 % input-token savings across tool calls.
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
            # Drop col_stats — already used to write the Python code, not needed for final answer
            new_content = json.dumps(
                {
                    "success": data.get("success"),
                    "cached": data.get("cached"),
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


def _group_into_turns(messages: list) -> list[list]:
    """
    Split a flat message list into per-turn sublists.

    Each turn starts with a HumanMessage.  Messages that appear before the
    first HumanMessage (unusual but possible) form their own leading group.
    """
    turns: list[list] = []
    current: list = []
    for msg in messages:
        if isinstance(msg, HumanMessage):
            if current:
                turns.append(current)
            current = [msg]
        else:
            current.append(msg)
    if current:
        turns.append(current)
    return turns


def _summarize_previous_turn(turn_msgs: list) -> list:
    """
    Compress a previous turn's internal tool-call chain while keeping the
    final agent answer intact.

    Returns one of:
      [HumanMessage, AIMessage(tool_summary), AIMessage(final_answer)]  — tools used
      [HumanMessage, AIMessage(final_answer)]                           — no tools
      [HumanMessage]                                                    — no answer yet

    Only the "internal" machinery is compressed:
      AIMessage(tool_calls=[clickhouse_query/python_analysis]) → dropped
      ToolMessage(query result / analysis output)              → dropped

    The final AIMessage — the one shown to the user in the UI — is preserved
    verbatim so the LLM has full context for drill-down follow-ups.

    The OpenAI-compatible API requires no pairing of tool_calls with tool
    messages in historical context.  Removing both together leaves a clean
    role-alternating sequence the API accepts without errors.

    Typical token profile per turn (10-column table, 200-token final answer):
      Before: HumanMessage + tool chain  ≈ 830 tokens + final answer ≈ 200 tokens
      After:  HumanMessage + tool_summary ≈ 50 tokens + final answer ≈ 200 tokens
    """
    human_msg: HumanMessage | None = None
    sql_snippet = ""
    row_info = ""
    final_ai_msg: AIMessage | None = None

    for msg in turn_msgs:
        if isinstance(msg, HumanMessage):
            human_msg = msg

        elif isinstance(msg, AIMessage):
            # Compact: extract SQL from tool_calls (internal machinery).
            for tc in getattr(msg, "tool_calls", []):
                if tc.get("name") == "clickhouse_query" and not sql_snippet:
                    sql = (tc.get("args") or {}).get("sql", "")
                    if sql:
                        sql_snippet = sql[:120] + ("…" if len(sql) > 120 else "")

            # Keep: identify the final answer (AIMessage without tool_calls).
            if not getattr(msg, "tool_calls", None):
                content = msg.content
                has_text = (isinstance(content, str) and content.strip()) or (
                    isinstance(content, list)
                    and any(
                        isinstance(b, dict)
                        and b.get("type") == "text"
                        and b.get("text", "").strip()
                        for b in content
                    )
                )
                if has_text:
                    final_ai_msg = msg  # last non-tool AIMessage wins

        elif isinstance(msg, ToolMessage):
            # Compact: extract just the row count (small metadata).
            try:
                data = json.loads(msg.content)
                if (getattr(msg, "name", "") or "") == "clickhouse_query" and not row_info:
                    rc = data.get("row_count")
                    cols = data.get("columns") or []
                    row_info = f"{rc} rows" if rc is not None else ""
                    if cols:
                        row_info += f", cols: {', '.join(str(c) for c in cols[:6])}"
            except Exception:
                pass

    result: list = []
    if human_msg is not None:
        result.append(human_msg)

    # Compact tool-chain summary (only added when tools were actually called).
    tool_parts: list[str] = []
    if sql_snippet:
        tool_parts.append(f"SQL: {sql_snippet}")
    if row_info:
        tool_parts.append(row_info)
    if tool_parts:
        result.append(AIMessage(content=" | ".join(tool_parts)))

    # Full final answer — kept verbatim, not truncated.
    if final_ai_msg is not None:
        result.append(final_ai_msg)

    return result if result else [AIMessage(content="—")]


def _build_schema_block(tables: list[dict]) -> str:
    """
    Format a list of {table, columns} dicts into a compact schema section
    for embedding in the system prompt.

    If columns contain type info, it is included for accurate Python code generation:
      **orders**: id UInt64, date Date, amount Decimal64(18,2), status String
    Otherwise (legacy format, column names only):
      **orders**: id, user_id, date, amount, status
    """
    lines = []
    for t in tables:
        cols = t.get("columns", [])
        if cols and isinstance(cols[0], dict):
            if "type" in cols[0]:
                col_parts = [f"{c['name']} {c['type']}" for c in cols]
            else:
                col_parts = [c["name"] for c in cols]
        else:
            col_parts = [str(c) for c in cols]
        lines.append(f"**{t['table']}**: {', '.join(col_parts)}")
    return "\n".join(lines)


# ─── System Prompt template ────────────────────────────────────────────────────
# {schema_section} is filled at agent startup with the live schema or a fallback.

_SYSTEM_PROMPT_TEMPLATE = """Ты — лучший в мире аналитик рекламных данных. Работаешь с ClickHouse-базой компании.
Твоя задача — отвечать на вопросы маркетолога по данным: трафик, покупки, кампании, поведение клиентов.

Стиль работы: ты находишься внутри рабочего процесса — маркетолог работает с данными каждый день, задаёт много вопросов подряд, возвращается к предыдущим темам, уточняет. Ты часть этого потока, не разовый отчёт. Отвечай коротко и по делу — как коллега, который уже в контексте.

## Схема базы данных

{schema_section}

### Принцип работы.
Ты ведёшь расследование, а не отвечаешь на изолированные вопросы. Держи нить:
* Помни что уже выяснили в этой сессии — не повторяй, опирайся
* Если данные противоречат здравому смыслу — скажи первым, не жди вопроса
* После ответа — одной строкой назови следующий логичный шаг. Не спрашивай "хочешь посмотреть?" — говори "следующий шаг: X"
* Не принимай данные за истину без проверки: аномалия, малая выборка, методология фильтрации — всё под сомнением пока не объяснено

## Рабочий процесс (выполняй строго по порядку):

### 1. Понять запрос
Определи тип — от этого зависит всё остальное:

- Факт ("сколько", "покажи", "топ") → одна цифра или таблица, без выводов
- Анализ ("почему", "сравни", "есть ли разница") → данные + 1–2 инсайта
- Интерпретация ("это норма?", "хорошо или плохо?") → одна витрина + маркетинговая логика, без тяжёлых JOIN-ов
- Drill-down ("разбери", "детализируй") → до первого запроса определи структуру финального ответа: какие витрины нужны, сколько запросов и в каком порядке. Запросы — следствие структуры ответа, не наоборот. Затем выполняй.
- Уточнение к предыдущему → сначала проверь, можно ли ответить из уже выгруженных данных; в базу — только если нет

### 1.5. Оценить объём

Лимит: 8 итераций (каждый вызов инструмента = 1 итерация).
Оцени объём сразу после понимания задачи, до первого запроса:
- Если укладываешься → выполняй полностью
- Если не укладываешься → раздели на логически завершённые части. Каждая часть самодостаточна: законченная таблица, законченный вывод, законченный график. Никогда не останавливайся на середине таблицы, списка или мысли.
  Начни ответ со строки: "Задача большая, разобью на N частей. Сейчас — часть 1: [что делаю]."
  В конце каждой части добавь: ⏭ Часть [X] из [N]: [что будет дальше] — задай следующим вопросом, я помню контекст.

### 2. Схема таблиц

Схема базы данных уже предоставлена в начале этого промпта — НЕ вызывай list_tables.
Используй list_tables только если схема кажется неполной или таблица не найдена.

### 3. Выгрузить данные из ClickHouse
Вызови clickhouse_query с оптимальным SQL:
* Агрегируй данные прямо в SQL (SUM, COUNT, AVG, GROUP BY) — ClickHouse очень быстр
* Фильтруй в WHERE — не выгружай лишнее
* LIMIT: обычно 1000–10000; до 500000 для больших выборок (таблицы могут содержать 800 000+ строк)
* Функции: toStartOfMonth(), toYear(), toDayOfWeek(), arrayJoin() и т.д.
* **Проверенные шаблоны фильтрации по датам** — используй именно этот синтаксис, он гарантированно работает:
  ```sql
  -- Прошлый месяц:
  WHERE date >= toStartOfMonth(today() - INTERVAL 1 MONTH) AND date < toStartOfMonth(today())
  -- Последние 30 дней:
  WHERE date >= today() - INTERVAL 30 DAY
  -- Текущий год:
  WHERE toYear(date) = toYear(today())
  -- Конкретный период:
  WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
  ```
  Не используй CTE только для фильтрации по дате — это всегда решается в WHERE напрямую.
* Сохрани parquet_path из ответа для python_analysis
* Если ответ содержит `"cached": true` — данные взяты из кэша, ClickHouse не был запрошен; итерация не потрачена
* Если уже есть parquet_path из предыдущего clickhouse_query — передай его напрямую в python_analysis, не повторяй тот же запрос
* КРИТИЧНО: объединяй данные из нескольких таблиц в ОДНОМ запросе через WITH/CTE — не делай несколько отдельных запросов туда и обратно:
  ```sql
  WITH кампании AS (
      SELECT campaign_id, SUM(spend) AS spend FROM dm_campaigns WHERE date >= '2024-01-01' GROUP BY campaign_id
  ),
  сессии AS (
      SELECT campaign_id, COUNT() AS visits, SUM(revenue) AS revenue FROM dm_traffic GROUP BY campaign_id
  )
  SELECT к.campaign_id, к.spend, с.visits, с.revenue, с.revenue / к.spend AS roas
  FROM кампании к LEFT JOIN сессии с USING (campaign_id)
  ```
* Начинай с одной витрины. JOIN — только если без него принципиально не решить
* При JOIN двух витрин — одной строкой укажи по какому ключу соединяешь и что это означает для интерпретации.
* При временной фильтрации — указывай в ответе по какому полю фильтруешь
* Если используешь другую таблицу вместо той, что назвал пользователь — первой строкой ответа объясни почему (например: "dm_campaign_funnel не содержит недельной динамики, использую dm_traffic_performance")

### 4. Проанализировать данные в Python
Вызови python_analysis для расчётов:
* Формируй Markdown-таблицы
* Считай метрики (CTR, CPC, CPM, ROAS, CR, CPA)
* Устанавливай переменную result с итоговым Markdown-выводом
* График — только если вопрос про динамику, тренд или сравнение нескольких сущностей. На факты и разовые цифры — не нужен

При анализе:
* Если n < 5 — помечай ⚠️, выводов не строить
* При ранжировании по среднему чеку или CR — всегда показывай n (количество заказов/сессий), иначе топ статистически бессмысленен
* Аномалии — исследуй, не игнорируй. Одна строка с 90% выручки — это сигнал, не норма
* Если данных для вывода недостаточно — скажи это прямо и предложи следующий шаг
* Перед любым делением и любой воронкой — проверь совместимость единиц. В dm_campaign_funnel два несмешиваемых трека: сессионный (visits → pre_purchase_visits → sessions_with_purchase) и клиентский (unique_clients_pre_purchase → unique_buyers). Делить одно на другое нельзя. Результат >100% — маркер этой ошибки, не аномалия данных.

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

Если вопрос требует данных которых нет в витринах (например, расходы, показы, ставки) — скажи прямо: "Для этого нужен Директ. Сейчас недоступен."

Если данных недостаточно → предложи следующий шаг: "Проверить CR этой кампании?"

Проактивность: если в данных виден нетривиальный паттерн — назови его, даже если не спросили. Один инсайт сверх вопроса — норма. Два и больше — уже балласт.

Каждое слово в выводе должно нести смысл. Никаких итоговых блоков с эмодзи, повторов, обобщений ради обобщений.

## Правила Python-кода:
1. df уже загружен — НЕ вызывай pd.read_parquet()
2. ВСЕГДА устанавливай result (Markdown строка с итогом)
3. Используй print() для логирования шагов: print("📊 Шаг 1: ...")
4. Подписывай графики на РУССКОМ: plt.title(), plt.xlabel(), plt.ylabel()
5. Форматируй числа: f"{{value:,.0f}}" (целые), f"{{value:,.2f}}" (дробные)
6. Обрабатывай пропуски: df.dropna() или df.fillna(0)
7. Для каждого графика — plt.tight_layout() перед следующим
8. График строй ТОЛЬКО если он явно нужен по типу вопроса (см. шаг 4) — не по умолчанию
9. Типы данных — sandbox автоматически конвертирует object-столбцы, но если тип неожиданный:
   - Даты: `df['col'] = pd.to_datetime(df['col'], errors='coerce')` → затем `.dt.year`, `.dt.month`
   - Числа: `df['col'] = pd.to_numeric(df['col'], errors='coerce')`
   - Проверяй: `print(df_info)` — словарь {{колонка: тип}} для быстрой диагностики
   - col_stats в ответе clickhouse_query содержит реальные типы pandas — ориентируйся на них
   - ЗАПРЕЩЕНО: вызывать python_analysis только для df.shape / dtypes / head() — col_stats уже содержит все эти данные. Каждый вызов python_analysis должен производить вычисления или строить таблицу для ответа.
10. Перед делением — всегда проверяй знаменатель: `df[df['знаменатель'] > 0]` или `.replace(0, np.nan)`
11. Строковые колонки с NULL/NaN — никогда не пиши `if row['field']` в `.apply()`: это сломается на NaN. Безопасный паттерн для создания меток:
    ```python
    df['label'] = df['utm_campaign'].apply(
        lambda v: str(v) if pd.notna(v) and str(v).strip() else 'unknown'
    )
    ```

## Рекламные метрики
Формулы не нужно воспроизводить в каждом ответе. Но:
* Если считаешь нестандартную метрику или производную — покажи формулу один раз
* Если вводишь аббревиатуру которую маркетолог мог не знать — расшифруй при первом упоминании
* Если данных для метрики нет (нет расхода → нет CPC/CPA/ROAS) — скажи прямо, не додумывай

## Справочник значений полей

### deviceCategory — тип устройства
1 — десктоп
2 — мобильные телефоны
3 — планшеты
4 — TV

При фильтрации или группировке по deviceCategory всегда расшифровывай цифры в читаемые названия в итоговых таблицах/ответах.

## Расхождение визитов между витринами — норма

dm_traffic_performance считает ВСЕ визиты, включая анонимные (clientID = 0).
dm_client_journey / dm_client_profile / dm_ml_features — только clientID > 0.
Разница = анонимные сессии. Это архитектурное решение, не ошибка данных.

Если пользователь замечает расхождение визитов между витринами — объясни это, не паникуй и не ищи баги.

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
    Return a ChatOpenAI client pointed at OpenRouter for the selected model.

    OpenRouter exposes a single OpenAI-compatible endpoint for all providers.
    ChatAnthropic is NOT used because OpenRouter does not implement the native
    Anthropic API (/v1/messages) — only the OpenAI format (/v1/chat/completions).

    Prompt caching for Claude models is still supported: OpenRouter forwards
    cache_control blocks found in message content to Anthropic transparently.
    The is_anthropic flag in the agent controls whether those blocks are added.

    For Anthropic: OpenRouter is pinned to a single Anthropic provider instance
    via the `provider` request field.  Without pinning, OpenRouter may
    round-robin across multiple Anthropic endpoints; different instances do NOT
    share the prompt cache, so every tool call within the same request would be
    a cache miss despite correct cache_control placement.
    """
    kwargs: dict = dict(
        model=MODEL,
        api_key=OPENROUTER_API_KEY,
        base_url="https://openrouter.ai/api/v1",
        max_tokens=MAX_TOKENS,
        default_headers=_OPENROUTER_HEADERS,
    )
    if MODEL_PROVIDER == "anthropic":
        kwargs["extra_body"] = {
            "provider": {
                "order": ["Anthropic"],
                "allow_fallbacks": False,
            },
        }
    return ChatOpenAI(**kwargs)


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
        # LLM call.  Applies four layered optimisations:
        #
        #   1. Sliding window — keep only the last MAX_HISTORY_TURNS human turns.
        #
        #   2. Turn summarisation — each previous turn's internal tool-call
        #      chain (AIMessage+tool_calls and ToolMessages) is replaced by a
        #      compact AIMessage containing only the SQL snippet and row count
        #      (~50 tokens).  The final agent answer (shown to the user in the
        #      UI) is kept verbatim as a separate AIMessage so the LLM has full
        #      context for drill-down follow-ups.
        #      The OpenAI API requires no pairing between tool_calls and tool
        #      messages in historical context — removing both together is valid.
        #
        #   3. Intra-turn ToolMessage compression — within the CURRENT request,
        #      once python_analysis has been called, strip dtypes + preview from
        #      preceding clickhouse_query results (already used to write the
        #      Python code, not needed for the final answer).  Also strips stdout
        #      from earlier python_analysis runs in a retry scenario.
        #
        #   4. Prompt caching (Anthropic only) — three cache breakpoints:
        #      a) system prompt, b) last compressed-history message, c) current
        #      HumanMessage (original text only, counter block is uncached).
        #      Breakpoints (a) and (b) cover [system + history]; breakpoint (c)
        #      additionally caches the user's question across all tool calls
        #      within the same turn (~68 %+ input-token savings).
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
            # current_turn_start = index of the last HumanMessage.
            current_turn_start = 0
            for i, msg in enumerate(messages):
                if isinstance(msg, HumanMessage):
                    current_turn_start = i

            # ── 2. Summarise previous turns ────────────────────────────────
            # Replace each past turn's tool-call chain with a compact
            # [HumanMessage, AIMessage(summary)] pair.
            prev_turns = _group_into_turns(messages[:current_turn_start])
            compressed_prev: list = []
            for turn in prev_turns:
                compressed_prev.extend(_summarize_previous_turn(turn))

            # ── 3. Intra-turn ToolMessage compression ──────────────────────
            # Within the current request, once python_analysis has been called,
            # the preceding clickhouse_query result's heavy fields (dtypes,
            # preview_first_5_rows) are no longer needed for the next LLM call.
            # Similarly, an earlier python_analysis stdout can be dropped when
            # a retry follows it.
            # Condition: compress msg[i] if ANY python_analysis ToolMessage
            # exists at index j > i in the current turn.
            current_msgs = messages[current_turn_start:]
            py_positions: set[int] = {
                i
                for i, m in enumerate(current_msgs)
                if isinstance(m, ToolMessage)
                and (getattr(m, "name", "") or "") == "python_analysis"
            }
            compressed_current: list = []
            for i, msg in enumerate(current_msgs):
                if (
                    isinstance(msg, ToolMessage)
                    and (getattr(msg, "name", "") or "") in ("clickhouse_query", "python_analysis")
                    and any(j > i for j in py_positions)
                ):
                    compressed_current.append(_compress_tool_message(msg))
                else:
                    compressed_current.append(msg)

            # ── 3b. Iteration counter + HumanMessage cache breakpoint ──────
            # The counter tells the LLM how many tool calls remain.
            # We modify only the local copy — the checkpoint is never touched.
            #
            # For Anthropic: the current HumanMessage is converted to a
            # content-block list so its original text is marked with
            # cache_control (third breakpoint after system + last history).
            # This caches the user's question across ALL tool calls within the
            # same turn — a new cache entry is established on the very first
            # LLM call and hits on every subsequent call.
            # The iteration counter is appended as a separate uncached block
            # so it can change without invalidating the cached prefix.
            #
            # For other providers: append the counter as a plain string
            # (original behaviour — no content-block support needed).
            tool_uses_so_far = sum(
                1 for m in compressed_current if isinstance(m, ToolMessage)
            )
            remaining = MAX_AGENT_ITERATIONS - tool_uses_so_far
            if compressed_current and isinstance(compressed_current[0], HumanMessage):
                first = compressed_current[0]
                old_content = first.content if isinstance(first.content, str) else ""
                if is_anthropic:
                    # Original text — stable, gets cached from the very first call.
                    content_blocks: list = [
                        {
                            "type": "text",
                            "text": old_content,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ]
                    # Counter block — changes each iteration, stays outside cache.
                    if tool_uses_so_far > 0:
                        counter = f"[⚡ Итерации: {tool_uses_so_far}/{MAX_AGENT_ITERATIONS}, осталось: {remaining}]"
                        if remaining <= 3:
                            counter += " — если данных достаточно, давай финальный ответ прямо сейчас."
                        content_blocks.append({"type": "text", "text": counter})
                    try:
                        compressed_current[0] = first.model_copy(
                            update={"content": content_blocks}
                        )
                    except Exception:
                        new_first = copy(first)
                        new_first.content = content_blocks
                        compressed_current[0] = new_first
                elif tool_uses_so_far > 0:
                    counter = f"\n[⚡ Итерации: {tool_uses_so_far}/{MAX_AGENT_ITERATIONS}, осталось: {remaining}]"
                    if remaining <= 3:
                        counter += " — если данных достаточно, давай финальный ответ прямо сейчас."
                    try:
                        compressed_current[0] = first.model_copy(
                            update={"content": old_content + counter}
                        )
                    except Exception:
                        new_first = copy(first)
                        new_first.content = old_content + counter
                        compressed_current[0] = new_first

            # ── 4. History cache breakpoint (Anthropic only) ──────────────
            # Mark the last summary message before the current turn with
            # cache_control.  Anthropic caches [system + compressed history]
            # and reads it from cache on every tool call in the same request.
            if is_anthropic and compressed_prev:
                last_hist_msg = compressed_prev[-1]
                content = last_hist_msg.content
                if isinstance(content, str) and content:
                    new_content: list | None = [
                        {
                            "type": "text",
                            "text": content,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ]
                elif isinstance(content, list) and content:
                    new_content = list(content)
                    last_block = dict(new_content[-1])
                    last_block["cache_control"] = {"type": "ephemeral"}
                    new_content[-1] = last_block
                else:
                    new_content = None
                if new_content is not None:
                    try:
                        compressed_prev[-1] = last_hist_msg.model_copy(
                            update={"content": new_content}
                        )
                    except Exception:
                        new_msg = copy(last_hist_msg)
                        new_msg.content = new_content
                        compressed_prev[-1] = new_msg

            # System prompt: cached block for Anthropic, plain string otherwise.
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

            return [system_msg] + compressed_prev + compressed_current

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
        config = {"configurable": {"thread_id": session_id}, "recursion_limit": MAX_AGENT_ITERATIONS * 2 + 1}

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
                "_messages": messages,  # for passive observability logger only
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
        """
        Extract a compact log of tool calls made during the current run.

        Each entry includes:
          - tool: tool name
          - input: args (SQL up to 2000 chars, other strings up to 500)
          - success: bool from ToolMessage (if available)
          - row_count / cached: for clickhouse_query
          - error: for failed calls
        """
        last_human_idx = -1
        for i, msg in enumerate(messages):
            if isinstance(msg, HumanMessage):
                last_human_idx = i

        if last_human_idx < 0:
            return []

        # Build tool_call_id → ToolMessage map so we can attach outputs
        tool_results: dict[str, ToolMessage] = {}
        for msg in messages[last_human_idx:]:
            if isinstance(msg, ToolMessage):
                tc_id = getattr(msg, "tool_call_id", None)
                if tc_id:
                    tool_results[tc_id] = msg

        tool_calls: list[dict] = []
        for msg in messages[last_human_idx:]:
            if not isinstance(msg, AIMessage):
                continue
            for tc in getattr(msg, "tool_calls", []):
                name = tc.get("name", "")
                args = tc.get("args", {})
                tc_id = tc.get("id", "")
                # SQL gets 2000 chars; other strings get 500
                compact_args = {
                    k: (
                        v[:2000] + "…" if k == "sql" and isinstance(v, str) and len(v) > 2000
                        else v[:500] + "…" if isinstance(v, str) and len(v) > 500
                        else v
                    )
                    for k, v in args.items()
                }
                entry: dict = {"tool": name, "input": compact_args}

                # Attach output metadata from ToolMessage
                tm = tool_results.get(tc_id)
                if tm is not None:
                    try:
                        data = json.loads(tm.content)
                        entry["success"] = data.get("success")
                        if name == "clickhouse_query":
                            entry["row_count"] = data.get("row_count")
                            entry["cached"] = data.get("cached")
                            if not data.get("success"):
                                entry["error"] = data.get("error", "")
                        elif name == "python_analysis":
                            if not data.get("success"):
                                entry["error"] = data.get("error", "")
                    except Exception:
                        entry["output_raw"] = str(tm.content)[:500]

                tool_calls.append(entry)

        return tool_calls


# ─── Global singleton ─────────────────────────────────────────────────────────
_agent: Optional[AnalyticsAgent] = None


def get_agent() -> AnalyticsAgent:
    """Return (or create) the global AnalyticsAgent instance."""
    global _agent
    if _agent is None:
        _agent = AnalyticsAgent()
    return _agent
