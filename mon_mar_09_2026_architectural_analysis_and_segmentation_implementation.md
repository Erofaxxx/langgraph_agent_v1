# Copilot Chat Conversation Export: Architectural analysis and segmentation implementation

**User:** @vdubrovin1704-ops
**Thread URL:** https://github.com/copilot/c/a595eaf5-e613-49c4-b6b8-08fa20fc3f88

## @vdubrovin1704-ops

прочитай репозиторий, ии-агент на langgraph. 

Нужно добавить в функционал агента, чтобы маркетолог в отдельном чате с агентом мог сформировать сегмент аудитории, этот сегмент сохранился в виде json , где есть название, описание сегмента и какой sql запрос надо давать в базу данных clickhouse, чтобы получить этот сегмент. В этом режиме сбора сегмента агент должен спросить у маркетолога много вопросов по поводу описания этого сегмента, чтобы построить этот сегмент, при этом агенту не нужено получается много контекста из других знаний, ему нужно просто построить сегмент. И убедиться, что задав такой запрос clickhouse этот сегмент построится верно. 

Как на базе архитектуры нынешнего агента, сделать такую возможность? Возможно это просто новый skill, а может есть более эффективный вариант решения? Прочитай segmentation_skill_spec.md, там подробно расписано, но rag реализовывать не нужно, нужно эти json сохранять внутри на сервере. 

Не жалей размышлений, сначала подумай хорошо, прочитай достаточно файлов из репозитория и затем ответь. 

## Copilot

Теперь у меня полная картина архитектуры. Давай детально разберём всё и дам тебе исчерпывающий архитектурный ответ.

---

## Анализ архитектуры и рекомендации по реализации сегментации

### Что я прочитал

Архитектура агента очень хорошо спроектирована:
- **LangGraph StateGraph**: `router_node → agent_node ⇄ tools_node`
- **Skills system**: роутер (Haiku) классифицирует запрос → загружает нужные `.md` файлы в системный промпт
- **SqliteSaver**: память сессий в `chat_history.db`
- **Инфраструктура**: FastAPI + async job queue + ChatLogger

Ключевой принцип добавления нового функционала: **создай `.md` файл + одну запись в `_registry.py` — код агента трогать не нужно**.

---

### Основной вопрос: просто новый skill или отдельный агент?

**Это НЕ просто новый skill**, и вот почему:

| Критерий | Обычный skill (кампании, когорты) | Сегментация |
|---|---|---|
| Режим работы | Агент отвечает на вопрос и завершает | Многоходовый диалог (5–10 вопросов) |
| Цель сессии | Аналитика + инсайты | Конструирование + сохранение артефакта |
| Контекст | Нужны данные ClickHouse | Нужен диалог с пользователем |
| Сохранение | Ничего не сохраняется | Сохраняется JSON-объект в SQLite |
| Инструменты | `clickhouse_query` + `python_analysis` | Нужен новый `save_segment` tool |
| Поведение ЛЛМ | «Расследование» | «Конструктор/интервьюер» |

---

### Рекомендуемый подход: Отдельный режим сессии + новый skill + новый tool

Это самый **минимальный по изменениям кода** вариант, который при этом даёт правильное поведение.

#### Архитектура решения

```
Frontend (Lovable)
      │
      ├─ POST /api/analyze          ← обычный аналитический чат (сессия mode=analytics)
      └─ POST /api/segment/build    ← отдельный endpoint для режима сегментации (сессия mode=segmentation)
             │
             └─ SegmentBuilderAgent (отдельный граф или тот же граф с другим системным промптом)
                    │
                    ├─ Спрашивает маркетолога вопросы
                    ├─ Формирует JSON-определение сегмента
                    ├─ Вызывает clickhouse_query для проверки (pробный подсчёт)
                    └─ Вызывает save_segment (новый tool) для сохранения
```

**Почему не трогаем существующий `AnalyticsAgent`:**
- У него другой системный промпт («ты аналитик, веди расследование»)
- Его поведение «После ответа — назови следующий логичный шаг» не подходит для диалога-конструктора
- Слайдинг-окно и компрессия заточены под аналитические сессии

---

### Что конкретно добавить: 5 файлов

#### 1. `segment_store.py` — хранилище сегментов в SQLite

```python name=clickhouse_analytics_agent/segment_store.py
"""
SegmentStore — хранилище именованных сегментов в SQLite.

Сегменты хранятся в той же chat_history.db (отдельная таблица `segments`).
JSON-схема каждого сегмента соответствует segmentation_skill_spec.md §5.

При переходе на RAG — только этот модуль меняется, агент не трогается.
"""

import json
import sqlite3
import threading
import uuid
from datetime import date, datetime, timezone
from typing import Optional

from config import DB_PATH


class SegmentStore:
    """Thread-safe CRUD для сегментов аудитории."""

    def __init__(self, db_path: str = DB_PATH) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS segments (
                    segment_id   TEXT PRIMARY KEY,
                    name         TEXT NOT NULL UNIQUE,
                    description  TEXT,
                    approach     TEXT,
                    period_json  TEXT,
                    conditions_json TEXT,
                    primary_table TEXT,
                    join_tables_json TEXT,
                    sql_query    TEXT,
                    last_count   INTEGER,
                    last_materialized TEXT,
                    used_in_json TEXT,
                    created_at   TEXT NOT NULL,
                    updated_at   TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_segments_name ON segments(name);
            """)
            self._conn.commit()

    def save(self, segment: dict) -> dict:
        """Сохранить или обновить сегмент. Возвращает сохранённый объект."""
        now = datetime.now(timezone.utc).date().isoformat()
        seg_id = segment.get("segment_id") or f"seg_{uuid.uuid4().hex[:8]}"

        with self._lock:
            self._conn.execute("""
                INSERT INTO segments (
                    segment_id, name, description, approach,
                    period_json, conditions_json, primary_table,
                    join_tables_json, sql_query, last_count,
                    last_materialized, used_in_json, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(segment_id) DO UPDATE SET
                    name=excluded.name,
                    description=excluded.description,
                    approach=excluded.approach,
                    period_json=excluded.period_json,
                    conditions_json=excluded.conditions_json,
                    primary_table=excluded.primary_table,
                    join_tables_json=excluded.join_tables_json,
                    sql_query=excluded.sql_query,
                    last_count=excluded.last_count,
                    last_materialized=excluded.last_materialized,
                    used_in_json=excluded.used_in_json,
                    updated_at=excluded.updated_at
            """, (
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
            ))
            self._conn.commit()

        segment["segment_id"] = seg_id
        segment["updated_at"] = now
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
        for key in ("period_json", "conditions_json", "join_tables_json", "used_in_json"):
            col = key.replace("_json", "") if key != "join_tables_json" else "join_tables"
            if key == "period_json": col = "period"
            if key == "conditions_json": col = "conditions"
            if key == "used_in_json": col = "used_in"
            try:
                d[col] = json.loads(d.pop(key) or "{}")
            except Exception:
                d[col] = {}
        return d


# ─── Singleton ────────────────────────────────────────────────────────────────
_store: Optional[SegmentStore] = None

def get_segment_store() -> SegmentStore:
    global _store
    if _store is None:
        _store = SegmentStore()
    return _store
```

---

#### 2. `tools_segmentation.py` — новый tool `save_segment`

```python name=clickhouse_analytics_agent/tools_segmentation.py
"""
Segmentation tool: save_segment.

Вызывается агентом-сегментатором после подтверждения маркетологом.
Сохраняет JSON-определение сегмента в SQLite через SegmentStore.
"""
import json
from langchain_core.tools import tool
from segment_store import get_segment_store


@tool
def save_segment(segment_json: str) -> str:
    """
    Save an audience segment definition to persistent storage.

    Call this ONLY after the user has confirmed the segment definition.

    Args:
        segment_json: JSON string with the full segment object. Required fields:
            - name (str): human-readable segment name
            - description (str): what this segment represents
            - approach (str): rfm | funnel_behavioral | channel | cohort | product | multichannel
            - period (dict): {"type": "rolling|fixed|cohort|all_time", ...}
            - conditions (dict): rfm/traffic/behavior/geo_device/purchases/funnel blocks
            - primary_table (str): main ClickHouse table for materialization
            - sql_query (str): the verified SELECT query to materialize this segment
            - last_count (int): number of users from the trial COUNT query
            - join_tables (list): additional tables if needed

    Returns:
        JSON with {"success": true, "segment_id": "seg_XXXXXX", "name": "..."} or error.
    """
    try:
        segment = json.loads(segment_json)
        if not segment.get("name"):
            return json.dumps({"success": False, "error": "Field 'name' is required"})
        if not segment.get("sql_query"):
            return json.dumps({"success": False, "error": "Field 'sql_query' is required — run a trial COUNT query first"})

        store = get_segment_store()
        saved = store.save(segment)
        return json.dumps({
            "success": True,
            "segment_id": saved["segment_id"],
            "name": saved["name"],
            "last_count": saved.get("last_count"),
        }, ensure_ascii=False)
    except json.JSONDecodeError as e:
        return json.dumps({"success": False, "error": f"Invalid JSON: {e}"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})
```

---

#### 3. `segment_agent.py` — отдельный агент-сегментатор

```python name=clickhouse_analytics_agent/segment_agent.py
"""
SegmentBuilderAgent — специализированный агент для создания сегментов аудитории.

Отличия от AnalyticsAgent:
  - Другой системный промпт: интервьюер, не аналитик
  - Другой набор инструментов: clickhouse_query + save_segment (без python_analysis)
  - Нет компрессии истории (сессии короткие, 5–10 ходов)
  - Нет роутера skills (всегда один режим)
  - Тот же SqliteSaver (отдельный thread_id с префиксом "seg_")
"""

import json
import sqlite3
from typing import Annotated, Optional, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from config import DB_PATH, MAX_TOKENS, MODEL, MODEL_PROVIDER, OPENROUTER_API_KEY
from tools import clickhouse_query  # reuse existing tool
from tools_segmentation import save_segment


# ─── Tools для сегментатора ───────────────────────────────────────────────────
SEGMENT_TOOLS = [clickhouse_query, save_segment]

# ─── Системный промпт ─────────────────────────────────────────────────────────
_SEGMENT_SYSTEM_PROMPT = """Ты — специалист по сегментации аудитории. Твоя единственная задача — помочь маркетологу \
создать точный, проверенный сегмент пользователей для ClickHouse-базы.

## Доступные таблицы для сегментации

- **dm_client_profile** — профиль клиента: RFM, устройство, город, источник трафика (533 352 строки)
  Ключевые поля: client_id, days_since_last_visit, total_visits, total_revenue, has_purchased,
  purchase_count, first_utm_source, last_utm_source, last_device, last_city, first_visit_date
  
- **dm_client_journey** — путь клиента по визитам: поведение в воронке (851 910 строк)
  Ключевые поля: client_id, date, utm_source, device, bounce, page_views, has_purchase, is_last_before_purchase
  
- **dm_conversion_paths** — пути конверсии: мультиканальность, длина пути (533 352 строки)
  Ключевые поля: client_id, converted, path_length, channels_path, sources_path, conversion_window_days
  
- **dm_purchases** — покупки: товары, категории, суммы (9 815 строк)
  Ключевые поля: client_id, date, product_category, product_name, revenue
  
- **visits** — сырые визиты: цели, товары, поиск (851 910 строк)
  Ключевые поля: clientID, date, productsCategory, purchaseID, goalsID, deviceCategory, regionCity

## Алгоритм работы

### Шаг 1 — Собери информацию (задавай вопросы по очереди)

Обязательно уточни следующее (если не сказано):
1. **Имя сегмента** — как маркетолог хочет его называть?
2. **Цель** — для чего сегмент: ретаргетинг, еженедельный отчёт, атрибуция?
3. **Временное окно** — последние N дней / конкретный период / когорта / все время?
4. **Кто входит** — купившие или нет? Сколько визитов? Какое устройство? Из каких источников?
5. **Что делали** — смотрели конкретные категории товаров? Достигали целей Метрики?
6. **География** — конкретные города или все?

Задавай вопросы по 1–2 за раз, не засыпай сразу всеми. Если ответ очевиден из контекста — не спрашивай.

### Шаг 2 — Сформируй SQL и проверь

После сбора информации:
1. Выбери основную таблицу (dm_client_profile для большинства случаев)
2. Напиши SQL для подсчёта: `SELECT count() AS cnt FROM ... WHERE ...`
3. Вызови `clickhouse_query` с этим SQL
4. Покажи результат маркетологу: размер сегмента + SQL-запрос

### Шаг 3 — Покажи итоговое определение и попроси подтверждение

```
**Сегмент: [Название]**
Подход: [тип]
Период: [описание]
Условия:
  - [условие 1]
  - [условие 2]
Размер: ~[N] пользователей

[SQL-запрос для материализации]

Сохранить сегмент?
```

### Шаг 4 — Сохрани после "Да"

Только после явного подтверждения вызови `save_segment` с полным JSON-объектом.
SQL в поле `sql_query` должен возвращать `client_id` (не COUNT).

## Правила генерации SQL

### Выбор таблицы по подходу
- RFM, воронка, когорта, канал → `dm_client_profile`
- Мультиканальность, длина пути → `dm_conversion_paths`
- Покупки конкретных товаров → `dm_purchases`
- Просмотры категорий, цели Метрики → `visits`

### Временное окно
```sql
-- rolling N дней (для dm_client_profile используй days_since_last_visit)
WHERE days_since_last_visit <= {N}

-- rolling по дате визита (для visits, dm_client_journey)
WHERE date >= today() - INTERVAL {N} DAY

-- fixed период
WHERE date BETWEEN '{from}' AND '{to}'

-- когорта по первому визиту
WHERE toYYYYMM(first_visit_date) = {YYYYMM}
```

### Финальный SQL для сохранения (возвращает client_id)
```sql
SELECT DISTINCT client_id
FROM dm_client_profile
WHERE {условия}
LIMIT 500000
```

## Ограничения
- Никогда не сохраняй сегмент без подтверждения пользователя
- Никогда не сохраняй без SQL-запроса с проверенным COUNT
- Для канальной сегментации обязательно уточни: first_touch, last_touch или any_touch
- Максимум без LIMIT — 500K строк

## Стиль
- Вопросы задавай коротко и по-русски
- Не объясняй техническую реализацию, говори о бизнес-смысле
- После сохранения покажи: "Сегмент сохранён. Теперь вы можете: ..."
"""


class SegmentAgentState(TypedDict):
    messages: Annotated[list, add_messages]


class SegmentBuilderAgent:
    """Специализированный агент для создания и сохранения сегментов аудитории."""

    def __init__(self) -> None:
        if not OPENROUTER_API_KEY:
            raise ValueError("OPENROUTER_API_KEY is not set")

        self.llm = ChatOpenAI(
            model=MODEL,
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            max_tokens=MAX_TOKENS,
            default_headers={
                "HTTP-Referer": "https://server.asktab.ru",
                "X-Title": "ClickHouse Segment Builder",
            },
        )

        # Тот же DB, но отдельный connection
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.memory = SqliteSaver(conn)

        # Граф: agent ⇄ tools (без роутера — всегда один режим)
        tool_node = ToolNode(SEGMENT_TOOLS)

        def agent_node(state: SegmentAgentState) -> dict:
            system_msg = SystemMessage(content=_SEGMENT_SYSTEM_PROMPT)
            messages = [system_msg] + state["messages"]
            response = self.llm.bind_tools(SEGMENT_TOOLS).invoke(messages)
            return {"messages": [response]}

        def should_continue(state: SegmentAgentState) -> str:
            last = state["messages"][-1]
            if hasattr(last, "tool_calls") and last.tool_calls:
                return "tools"
            return END

        graph = StateGraph(SegmentAgentState)
        graph.add_node("agent", agent_node)
        graph.add_node("tools", tool_node)
        graph.set_entry_point("agent")
        graph.add_conditional_edges(
            "agent", should_continue, {"tools": "tools", END: END}
        )
        graph.add_edge("tools", "agent")
        self.graph = graph.compile(checkpointer=self.memory)

        print(f"✅ SegmentBuilderAgent ready | model: {MODEL} | db: {DB_PATH}")

    def chat(self, user_message: str, session_id: str) -> dict:
        """
        Send one message in a segmentation conversation.
        
        session_id should have prefix "seg_" to distinguish from analytics sessions.
        """
        config = {
            "configurable": {"thread_id": f"seg_{session_id}"},
            "recursion_limit": 20,
        }
        try:
            result = self.graph.invoke(
                {"messages": [HumanMessage(content=user_message)]},
                config=config,
            )
            messages = result.get("messages", [])
            
            # Извлечь финальный текст
            text_output = ""
            for msg in reversed(messages):
                if isinstance(msg, AIMessage):
                    content = msg.content
                    if isinstance(content, str) and content.strip():
                        text_output = content
                        break
                    if isinstance(content, list):
                        parts = [b["text"] for b in content if isinstance(b, dict) and b.get("type") == "text"]
                        text = "\n".join(parts).strip()
                        if text:
                            text_output = text
                            break
            
            # Проверить, был ли сохранён сегмент
            segment_saved = any(
                isinstance(msg, ToolMessage)
                and (getattr(msg, "name", "") or "") == "save_segment"
                for msg in messages
            )
            
            return {
                "success": True,
                "session_id": session_id,
                "text_output": text_output,
                "segment_saved": segment_saved,
                "error": None,
            }
        except Exception as exc:
            import traceback as tb
            return {
                "success": False,
                "session_id": session_id,
                "text_output": "",
                "segment_saved": False,
                "error": str(exc),
                "traceback": tb.format_exc(),
            }

    def get_session_history(self, session_id: str) -> list[dict]:
        """Вернуть историю диалога сессии (для фронтенда)."""
        try:
            config = {"configurable": {"thread_id": f"seg_{session_id}"}}
            state = self.graph.get_state(config)
            msgs = state.values.get("messages", []) if state and state.values else []
            history = []
            for msg in msgs:
                if isinstance(msg, HumanMessage):
                    history.append({"role": "user", "content": msg.content if isinstance(msg.content, str) else str(msg.content)})
                elif isinstance(msg, AIMessage) and not getattr(msg, "tool_calls", None):
                    content = msg.content
                    if isinstance(content, str) and content.strip():
                        history.append({"role": "assistant", "content": content})
            return history
        except Exception:
            return []


# ─── Singleton ────────────────────────────────────────────────────────────────
_segment_agent: Optional[SegmentBuilderAgent] = None

def get_segment_agent() -> SegmentBuilderAgent:
    global _segment_agent
    if _segment_agent is None:
        _segment_agent = SegmentBuilderAgent()
    return _segment_agent
```

---

#### 4. Новые endpoints в `api_server.py`

В существующий `api_server.py` добавить:

```python name=clickhouse_analytics_agent/api_server_segment_endpoints.py
# ── Добавить в api_server.py ───────────────────────────────────────────────

from segment_agent import get_segment_agent
from segment_store import get_segment_store

class SegmentChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

class SegmentChatResponse(BaseModel):
    success: bool
    session_id: str
    text_output: str
    segment_saved: bool
    error: Optional[str] = None


@app.post("/api/segment/chat", response_model=SegmentChatResponse)
async def segment_chat(req: SegmentChatRequest):
    """
    Диалог с агентом-сегментатором.
    Каждый вызов — один ход в многоходовом диалоге.
    session_id сохраняй между вызовами чтобы держать контекст.
    """
    session_id = req.session_id or str(uuid.uuid4())
    agent = get_segment_agent()
    
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, agent.chat, req.message, session_id
    )
    
    return SegmentChatResponse(
        success=result["success"],
        session_id=session_id,
        text_output=result.get("text_output", ""),
        segment_saved=result.get("segment_saved", False),
        error=result.get("error"),
    )


@app.get("/api/segments")
async def list_segments():
    """Список всех сохранённых сегментов."""
    store = get_segment_store()
    return {"segments": store.list_all()}


@app.get("/api/segments/{segment_id}")
async def get_segment(segment_id: str):
    """Получить сегмент по ID."""
    store = get_segment_store()
    seg = store.get_by_id(segment_id)
    if not seg:
        raise HTTPException(status_code=404, detail="Segment not found")
    return seg


@app.delete("/api/segments/{segment_id}")
async def delete_segment(segment_id: str):
    """Удалить сегмент."""
    store = get_segment_store()
    deleted = store.delete(segment_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Segment not found")
    return {"success": True}


@app.get("/api/segment/chat/{session_id}/history")
async def get_segment_chat_history(session_id: str):
    """История диалога сессии сегментации."""
    agent = get_segment_agent()
    history = agent.get_session_history(session_id)
    return {"session_id": session_id, "history": history}
```

---

#### 5. `skills/segmentation.md` — добавить skill в основной агент (опционально)

Это нужно если хочешь, чтобы в **аналитическом чате** агент мог ссылаться на существующие сегменты (например: *«покажи атрибуцию для сегмента "Тёплые лиды"»*). Это отдельная, более сложная фича.

```markdown name=clickhouse_analytics_agent/skills/segmentation.md
## Skill: Использование именованных сегментов (только чтение)

### Когда активируется
Запросы содержат: "сегмент", "аудитория", "покажи сегмент", "для сегмента", "лояльные покупатели"

### Что ты можешь делать
ТОЛЬКО использовать уже созданные сегменты для аналитики.
Создание новых сегментов — в отдельном чате.

### Как использовать сегмент в запросе
Когда пользователь ссылается на именованный сегмент (например "Тёплые лиды Direct"):
1. Сообщи что используешь этот сегмент
2. Используй SQL из поля `sql_query` этого сегмента как подзапрос:

```sql
-- Пример использования сегмента в атрибуции:
WITH segment AS (
    -- вставь sql_query сегмента сюда
    SELECT DISTINCT client_id FROM dm_client_profile
    WHERE first_utm_source = 'ya-direct' AND total_visits >= 2 AND has_purchased = 0
    AND days_since_last_visit <= 30
)
SELECT ... 
FROM dm_conversion_paths cp
WHERE cp.client_id IN (SELECT client_id FROM segment)
```

### Ограничения
- НЕ создавай и не редактируй сегменты — только используй существующие
- Если сегмент не найден — скажи пользователю создать его в режиме сегментации
```

И в `_registry.py` добавить:

```python name=clickhouse_analytics_agent/skills/_registry_addition.py
# Добавить в SKILLS в _registry.py:
"segmentation": {
    "router_hint": (
        "сегмент аудитории, именованный сегмент, для сегмента, "
        "лояльные покупатели, тёплые лиды, аудитория из сегмента, "
        "покажи сегмент, использовать сегмент, segment, audience"
    ),
    "full_path": _SKILLS_DIR / "segmentation.md",
},
```

---

### Итоговая схема архитектуры

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (Lovable)                      │
│                                                                 │
│  Чат 1: POST /api/analyze          Чат 2: POST /api/segment/chat│
│  (аналитика)                       (создание сегментов)        │
└──────────────┬───────────────────────────────┬──────────────────┘
               │                               │
               ▼                               ▼
┌─────────────────────────┐    ┌─────────────────────────────────┐
│   AnalyticsAgent        │    │   SegmentBuilderAgent           │
│   (существующий)        │    │   (новый)                       │
│                         │    │                                 │
│ router_node             │    │ agent_node                      │
│   → agent_node          │    │   ├─ clickhouse_query (проверка)│
│   ⇄ tools_node          │    │   └─ save_segment (сохранение)  │
│                         │    │                                 │
│ Skills: сегмент (read)  │    │ Промпт: интервьюер-конструктор  │
└────────────┬────────────┘    └──────────────┬──────────────────┘
             │                                │
             └──────────┬─────────────────────┘
                        │
                        ▼
            ┌───────────────────────┐
            │  SQLite chat_history.db│
            │                       │
            │  checkpoints (память) │
            │  agent_logs           │
            │  segments  ← НОВОЕ   │
            └───────────────────────┘
```

---

### Почему именно этот подход, а не альтернативы

| Альтернатива | Почему хуже |
|---|---|
| **Один агент + флаг `mode` в session** | Промпт аналитика и промпт интервьюера несовместимы. Агент будет путаться, что от него хотят в текущей сессии |
| **Новый skill в существующий агент** | Skill только добавляет инструкции в промпт, но не меняет поведение агента. «Назови следующий шаг» и «задавай по одному вопросу» — противоречащие паттерны |
| **Полный отдельный процесс** | Overkill — можно использовать тот же `clickhouse_query` tool, тот же SQLite |
| **Предложенный подход: отдельный граф + отдельный промпт** | ✅ Минимум нового кода (~200 строк), переиспользует существующие tools и DB, не трогает рабочий агент |

---

### Приоритет разработки

1. **`segment_store.py`** — ~100 строк, чистый SQLite CRUD
2. **`tools_segmentation.py`** — ~40 строк, один `@tool`
3. **`segment_agent.py`** — ~150 строк, граф без роутера
4. **Добавить endpoints в `api_server.py`** — ~60 строк
5. **`skills/segmentation.md` + registry** — 20 строк, чтобы аналитический агент мог ссылаться на сохранённые сегменты

Итого: **~370 строк нового кода**, существующий `agent.py`, `tools.py`, `config.py` **не меняются вообще**.

## @vdubrovin1704-ops

Исходя из твоего ответа и segmentation_skill_spec.md 
создай 1 большой md файл с инструкцией как внедрить этот дополнительный функционал, чтобы по этой инструкции claude code внёс изменения и реализовал этого подагента. 

*System: User retried message*

## Copilot

We're waiting for your response to the tool call to continue — specifically, whether you approve pushing this file to the repository.

## @vdubrovin1704-ops

Исходя из твоего ответа и segmentation_skill_spec.md 
создай 1 большой md файл с инструкцией как внедрить этот дополнительный функционал, чтобы по этой инструкции claude code внёс изменения и реализовал этого подагента. 

*System: User retried message*
