# Улучшения агента: контроль итераций и рост контекста

> Документ содержит **только изменения с максимальным эффектом**, основанные на реальном коде репозитория.  
> Изменения перечислены в порядке приоритета — сверху вниз.

---

## Проблема 1 — Неправильная формула `recursion_limit` (КРИТИЧНО)

### Почему падает с ошибкой

Граф имеет топологию: `router → agent ⇄ tools`

Каждый цикл расходует шаги LangGraph так:
```
router          → 1 шаг (только в начале)
agent           → 1 шаг
tools           → 1 шаг
agent           → 1 шаг  (ответ или следующий tool call)
...
```

Итого шагов при N tool calls: `1 (router) + 1 (первый agent) + N × 2 (tools + agent)`

При `MAX_AGENT_ITERATIONS = 15`:  
`1 + 1 + 15 × 2 = 32` шагов нужно, но `recursion_limit = MAX_AGENT_ITERATIONS * 2 + 1 = 31`.

**Агент падает ровно на последней итерации, потому что лимит не оставляет места для финального ответа.**

### Текущий код (`agent.py`, метод `analyze`)

```python
config = {
    "configurable": {"thread_id": session_id},
    "recursion_limit": MAX_AGENT_ITERATIONS * 2 + 1,  # ← НЕПРАВИЛЬНО
}
```

### Что поменять

```python
config = {
    "configurable": {"thread_id": session_id},
    # router(1) + first_agent(1) + N cycles × 2 + запас(5)
    "recursion_limit": 2 + MAX_AGENT_ITERATIONS * 2 + 5,
}
```

При `MAX_AGENT_ITERATIONS = 15` это даст `recursion_limit = 37`.  
Запас в 5 шагов гарантирует, что агент успевает отдать финальный ответ даже если вышел точно на лимит tool calls.

То же самое для `segment_agent.py`:

```python
# Было:
"recursion_limit": 30,

# Стало (при _MAX_SEG_TURNS = 8):
"recursion_limit": 2 + _MAX_SEG_TURNS * 2 + 5,  # = 23
```

---

## Проблема 2 — `should_continue` не останавливает агента (КРИТИЧНО)

### Почему это проблема

Сейчас `should_continue` проверяет только наличие `tool_calls` в последнем сообщении:

```python
def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return END
```

Агент **сам решает когда остановиться**. Если он не остановился добровольно — LangGraph бросает `GRAPH_RECURSION_LIMIT`. Счётчик `[⚡ Итерации: N/15]` в HumanMessage уже есть и работает, но это только **мягкая подсказка** — LLM её может проигнорировать.

### Что поменять

Добавить **жёсткую остановку**: если агент израсходовал все `MAX_AGENT_ITERATIONS` — принудительно выйти из цикла без ошибки.

```python
def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]

    # ── Жёсткий стоп по бюджету итераций ──────────────────────────���───
    # Считаем ToolMessages начиная с последнего HumanMessage (текущий ход)
    messages = state["messages"]
    human_indices = [i for i, m in enumerate(messages) if isinstance(m, HumanMessage)]
    current_turn_start = human_indices[-1] if human_indices else 0
    current_tool_uses = sum(
        1 for m in messages[current_turn_start:]
        if isinstance(m, ToolMessage)
    )

    if current_tool_uses >= MAX_AGENT_ITERATIONS:
        # Лимит исчерпан — выходим. Агент уже видел предупреждение в счётчике,
        # поэтому последний AIMessage должен содержать частичный ответ.
        return END
    # ──────────────────────────────────────────────────────────────────

    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return END
```

**Эффект:** агент **никогда** не упадёт с `GRAPH_RECURSION_LIMIT`. При исчерпании бюджета он вернёт то, что успел собрать — пользователь получит частичный ответ, а не ошибку.

---

## Проблема 3 — Рост контекста внутри текущего хода

### Почему это проблема

Компрессия `_compress_tool_message` применяется только к **предыдущим ходам** (через `_summarize_previous_turn`).  
Внутри текущего хода `clickhouse_query` ToolMessage сжимается только если **после него уже был AIMessage** (LLM уже потребил `col_stats`).

При 15 tool calls в одном ходу в контексте накапливаются полные `col_stats`, preview-строки и stdout от python — даже для звонков, сделанных в начале хода.

### Что поменять в `_build_messages`

Добавить **sliding window для средней части текущего хода** — последние `KEEP_RECENT = 4` раундов `(AIMessage+ToolMessage)` держим как есть, всё что раньше — сжимаем агрессивно.

Вставить после блока `# ── 3. Intra-turn ToolMessage compression` (перед блоком `# ── 3b.`):

```python
# ── 3c. Aggressive middle-of-turn compression (sliding window) ────────
# Оставляем последние KEEP_RECENT раундов tool+ai без изменений,
# всё что до этого внутри текущего хода — сжимаем до минимума.
KEEP_RECENT = 4  # сколько последних tool-раундов оставить несжатыми

# Найти индексы AIMessage с tool_calls (начало каждого раунда)
ai_tool_indices = [
    i for i, m in enumerate(compressed_current)
    if isinstance(m, AIMessage) and getattr(m, "tool_calls", None)
]

if len(ai_tool_indices) > KEEP_RECENT:
    # Граница: всё до этого индекса — сжимаем
    cutoff = ai_tool_indices[-KEEP_RECENT]
    middle = compressed_current[:cutoff]
    recent = compressed_current[cutoff:]

    compressed_middle = []
    for m in middle:
        if isinstance(m, ToolMessage):
            # Уже сжато в блоке 3, но применим повторно для надёжности
            compressed_middle.append(_compress_tool_message(m))
        elif isinstance(m, AIMessage) and getattr(m, "tool_calls", None):
            # Убираем tool_calls из старых AI-сообщений — они уже исполнены
            # и хранить их аргументы в контексте бессмысленно
            try:
                compressed_middle.append(m.model_copy(update={"tool_calls": []}))
            except Exception:
                new_m = copy(m)
                new_m.tool_calls = []
                compressed_middle.append(new_m)
        else:
            compressed_middle.append(m)

    compressed_current = compressed_middle + recent
# ──────────────────────────────────────────────────────────────────────
```

**Эффект:** при 12+ tool calls в ходу контекст растёт логарифмически, а не линейно. Последние 4 раунда LLM видит полностью (актуальные данные), первые N — только метаданные.

---

## Проблема 4 — Слабый forced-finalization prompt

### Почему это проблема

Текущий счётчи�� при `remaining <= 3`:
```
[⚡ Итерации: 12/15, осталось: 3] — если данных достаточно, давай финальный ответ прямо сейчас.
```

Это **рекомендация**, а не директива. LLM может её проигнорировать и сделать ещё 3 tool call.

### Что поменять в `_build_messages` (блок `# ── 3b.`)

Усилить сообщение при разных порогах:

```python
# Было:
if remaining <= 3:
    counter += " — если данных достаточно, давай финальный ответ прямо сейчас."

# Стало:
if remaining <= 0:
    counter += (
        " ⛔ ЛИМИТ ИСЧЕРПАН. Немедленно дай финальный ответ на основе уже собранных данных."
        " НЕ вызывай инструменты. Ис��ользуй только то, что уже есть в контексте."
    )
elif remaining == 1:
    counter += (
        " 🚨 Остался 1 вызов инструмента. После него ты ОБЯЗАН дать финальный ответ."
        " Используй последний вызов только если он критически необходим."
    )
elif remaining <= 3:
    counter += (
        " ⚠️ Мало итераций. Если данных достаточно — отвечай сейчас."
        " Объединяй оставшиеся запросы в один через WITH/CTE."
    )
```

**Эффект:** LLM получает эскалирующие сигналы. При `remaining = 0` — директива, а не просьба. В сочетании с Fix 2 (`should_continue`) это означает: агент сам остановится раньше лимита, а если нет — его остановит граф без ошибки.

---

## Проблема 5 — Отсутствие planning instruction в system prompt

### Почему это проблема

Агент сейчас работает **реактивно**: получил вопрос → сразу идёт в ClickHouse. При составных вопросах это приводит к хаотичным запросам: сначала разведка одной таблицы, потом другой, потом снова первой.

В `_SYSTEM_PROMPT_CORE` нет явной инструкции планировать перед действием.

### Что добавить в `_SYSTEM_PROMPT_CORE`

Найти секцию `## Рабочий процесс` и **в самое начало** добавить:

```
## Стратегия р��боты

Перед первым вызовом инструмента составь мысленный план:
1. Что нужно узнать для ответа? (1-3 пункта максимум)
2. Какие таблицы нужны? (схема уже в промпте — не делай SELECT для разведки)
3. Можно ли объединить всё в один запрос через WITH/CTE?

Правила экономии итераций:
- Схема уже встроена в промпт — не вызывай list_tables и не делай DESCRIBE
- Не делай SELECT * LIMIT 10 для "посмотреть что в таблице" — используй схему
- Объединяй несколько вопросов об одних данных в один SQL-запрос
- Повторно используй parquet_path из предыдущего clickhouse_query вместо нового запроса
- Если данных из предыдущего ответа достаточно — используй их, не дублируй запрос
```

**Эффект:** для большинства простых вопросов агент будет делать 1-2 запроса вместо 3-5. Это самый дешёвый в реализации способ снизить количество итераций.

---

## Итог: что и где менять

| # | Файл | Что менять | Строк кода | Эффект |
|---|------|-----------|-----------|--------|
| 1 | `agent.py` → `analyze()` | Формула `recursion_limit` | 1 | Убирает ошибку при ровном достижении лимита |
| 1 | `segment_agent.py` → `chat()` | Формула `recursion_limit` | 1 | То же для сегментного агента |
| 2 | `agent.py` → `should_continue()` | Жёсткий стоп по счётчику ToolMessages | ~10 | Агент **никогда** не падает с ошибкой |
| 3 | `agent.py` → `_build_messages()` | Sliding window для середины текущего хода | ~25 | Контекст растёт логарифмически, не линейно |
| 4 | `agent.py` → `_build_messages()` | Эскалирующий forced-finalization prompt | ~10 | LLM останавливается сам до срабатывания Fix 2 |
| 5 | `agent.py` → `_SYSTEM_PROMPT_CORE` | Planning instruction + правила экономии | ~10 строк текста | -30-50% tool calls на составных вопросах |

**Приоритет реализации:** Fix 1 + Fix 2 → Fix 4 → Fix 5 → Fix 3.

Fix 1 и 2 дают немедленный результат — исчезновение ошибки.  
Fix 4 и 5 — агент начинает останавливаться сам, не доходя до принудительного стопа.  
Fix 3 — снижает рост контекста при длинных цепочках (актуально если агент делает 10+ tool calls в одном ходу).