# Segment Builder API — Frontend Reference

Base URL: `https://your-server/api`

## Изоляция по пользователям

Все запросы к segment API принимают заголовок **`X-User-Id`** (строка — любой уникальный идентификатор пользователя из вашей auth-системы: email, user_id, UUID).

- Каждый пользователь видит **только свои** сегменты
- Удалить можно только свой сегмент (чужой вернёт 404)
- Без заголовка — сегменты попадают в общее пространство `__shared__`

```
X-User-Id: user@example.com
```

---

## 1. Чат с агентом-сегментатором

### `POST /api/segment/chat`

Один ход диалога. Сохраняй `session_id` между вызовами.

**Headers**
```
Content-Type: application/json
X-User-Id: user@example.com
```

**Request**
```json
{
  "message": "Хочу создать сегмент тёплых лидов из Яндекс.Директа",
  "session_id": "uuid-optional"
}
```

**Response**
```json
{
  "success": true,
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "text_output": "Понял! За какой период смотреть — последние 30 дней или конкретный диапазон?",
  "segment_saved": false,
  "error": null
}
```

| Поле | Тип | Описание |
|---|---|---|
| `session_id` | string | Передавай в каждом следующем сообщении |
| `text_output` | string | Ответ агента (Markdown) |
| `segment_saved` | bool | `true` если в этом ходу был сохранён сегмент |
| `error` | string\|null | Сообщение об ошибке |

**Типичный flow диалога (5–8 ходов):**

```
→ "Хочу сегмент тёплых лидов из директа"
← "За какой период?"
→ "Последние 30 дней"
← "Сколько визитов минимум?"
→ "2 и больше, без покупок"
← "**Сегмент: Тёплые лиды Direct**\n...SQL...\nРазмер: ~8 240. Сохранить?"
→ "Да"
← "Сегмент сохранён ✓"   ← segment_saved: true
```

---

### `GET /api/segment/chat/{session_id}/history`

История диалога сессии (для восстановления чата после перезагрузки страницы).
Заголовок `X-User-Id` не требуется — история хранится по session_id.

**Response**
```json
{
  "session_id": "550e8400-...",
  "history": [
    {"role": "user", "content": "Хочу сегмент тёплых лидов"},
    {"role": "assistant", "content": "За какой период?"},
    {"role": "user", "content": "Последние 30 дней"}
  ]
}
```

---

## 2. Управление сохранёнными сегментами

Все endpoints требуют заголовок `X-User-Id`.

### `GET /api/segments`

Список сегментов текущего пользователя (сортировка по дате обновления — новые первыми).

**Headers:** `X-User-Id: user@example.com`

**Response**
```json
{
  "segments": [
    {
      "segment_id": "seg_7a3f1c",
      "name": "Тёплые лиды Direct",
      "description": "Пользователи из ya-direct, 2+ визита, без покупок, 30 дней",
      "approach": "funnel_behavioral",
      "owner": "user@example.com",
      "period": {"type": "rolling", "days": 30},
      "conditions": {
        "rfm": {"frequency_min": 2, "has_purchased": false},
        "traffic": {"attribution_type": "first_touch", "utm_source": ["ya-direct"]}
      },
      "primary_table": "dm_client_profile",
      "join_tables": [],
      "sql_query": "SELECT DISTINCT client_id FROM dm_client_profile WHERE ...",
      "last_count": 8240,
      "last_materialized": "2026-03-09",
      "used_in": [],
      "created_at": "2026-03-09",
      "updated_at": "2026-03-09"
    }
  ]
}
```

**Ключевые поля для отображения в UI:**

| Поле | Где показывать |
|---|---|
| `name` | Заголовок карточки |
| `description` | Подзаголовок |
| `last_count` | "~8 240 пользователей" |
| `last_materialized` | "Актуально на 09.03.2026" |
| `approach` | Тег/бейдж (rfm, канальный, когортный…) |
| `sql_query` | Кнопка "Скопировать SQL" |

---

### `GET /api/segments/{segment_id}`

Полный объект одного сегмента. Возвращает 404 если сегмент не найден или принадлежит другому пользователю.

**Headers:** `X-User-Id: user@example.com`

**Response** — тот же объект сегмента без обёртки `segments: [...]`.

---

### `DELETE /api/segments/{segment_id}`

Удалить сегмент. Возвращает 404 если сегмент не найден или чужой.

**Headers:** `X-User-Id: user@example.com`

**Response**
```json
{"success": true}
```

---

## 3. Пример интеграции (псевдокод)

```typescript
const userId = getCurrentUser().email;  // или id из вашей auth

const authHeaders = {
  'Content-Type': 'application/json',
  'X-User-Id': userId,
};

// ── Чат сегментации ─────────────────────────────────────────────────

const [sessionId, setSessionId] = useState<string | null>(null);
const [messages, setMessages] = useState<Message[]>([]);

async function sendMessage(text: string) {
  const res = await fetch('/api/segment/chat', {
    method: 'POST',
    headers: authHeaders,
    body: JSON.stringify({message: text, session_id: sessionId}),
  });
  const data = await res.json();

  setSessionId(data.session_id);  // сохранить для следующего запроса
  setMessages(prev => [
    ...prev,
    {role: 'user', content: text},
    {role: 'assistant', content: data.text_output},
  ]);

  if (data.segment_saved) {
    // показать уведомление + обновить список сегментов
    refreshSegmentList();
  }
}

// Восстановить историю после перезагрузки страницы
async function restoreHistory(sessionId: string) {
  const res = await fetch(`/api/segment/chat/${sessionId}/history`);
  const {history} = await res.json();
  setMessages(history);
}

// ── Список сегментов ─────────────────────────────────────────────────

async function loadSegments() {
  const res = await fetch('/api/segments', {headers: authHeaders});
  const {segments} = await res.json();
  return segments;  // только сегменты текущего пользователя
}

async function deleteSegment(segmentId: string) {
  await fetch(`/api/segments/${segmentId}`, {
    method: 'DELETE',
    headers: authHeaders,
  });
  refreshSegmentList();
}
```

---

## 4. Использование сегмента в основном агенте

### Как работает основной агент

Основной аналитический агент — это **отдельный сервис** (`POST /api/analyze`), асинхронный:

```
POST /api/analyze           ← отправить запрос
  → { job_id, session_id }

GET  /api/job/{job_id}      ← polling до status: "done"
  → { status, text_output, plots, tool_calls, error }
```

> **Важно:** заголовок `X-User-Id` для основного агента **не нужен** — у него нет изоляции по пользователям. Передаётся только в segment API.

---

### Как передать сегмент в контекст

У основного агента **нет отдельного параметра** для сегмента — контекст передаётся **только через поле `query`**.

Когда пользователь выбирает сегмент, фронтенд **встраивает** его данные в начало запроса:

```
[Сегмент: "Тёплые лиды Direct"
sql_query: SELECT DISTINCT client_id FROM dm_client_profile WHERE first_utm_source = 'ya-direct' AND total_visits >= 2 AND has_purchased = 0 AND days_since_last_visit <= 30]

Покажи распределение по каналам для этого сегмента
```

Агент увидит SQL, активирует skill `segmentation` и сразу построит CTE — **не будет просить пользователя вставить SQL вручную**.

---

### UI-паттерн: тег `@mention`

**Пользователь видит** красивый тег в поле ввода:

```
[@Тёплые лиды Direct ×]  Покажи распределение по каналам
```

**Агент получает** query с встроенным SQL:

```
[Сегмент: "Тёплые лиды Direct"
sql_query: SELECT DISTINCT client_id ...]

Покажи распределение по каналам
```

**Триггеры для открытия выпадающего списка:**
- Кнопка `+` рядом с полем ввода
- Символ `@` в тексте → автодополнение по имени сегмента

---

### TypeScript: полный пример

```typescript
// ── Типы ────────────────────────────────────────────────────────────

interface SegmentMention {
  id: string;
  name: string;
  sql_query: string;
}

interface MainAgentRequest {
  query: string;
  session_id?: string;
}

interface JobStatusResponse {
  job_id: string;
  session_id: string;
  status: 'pending' | 'running' | 'done' | 'error';
  text_output?: string;
  plots?: string[];
  error?: string;
}

// ── Сборка query с инъекцией сегментов ──────────────────────────────

function buildQueryWithSegments(
  userText: string,
  mentions: SegmentMention[],
): string {
  if (mentions.length === 0) return userText;

  const blocks = mentions.map(
    (s) => `[Сегмент: "${s.name}"\nsql_query: ${s.sql_query}]`,
  );
  return `${blocks.join('\n')}\n\n${userText}`;
}

// ── Отправка в основной агент + polling ──────────────────────────────

async function sendToMainAgent(
  userText: string,
  mentions: SegmentMention[],
  sessionId: string | null,
): Promise<{ text_output: string; session_id: string }> {
  const query = buildQueryWithSegments(userText, mentions);

  // 1. Отправить запрос
  const submitRes = await fetch('/api/analyze', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, session_id: sessionId } satisfies MainAgentRequest),
  });
  const { job_id, session_id: newSessionId } = await submitRes.json();

  // 2. Polling до завершения
  while (true) {
    await new Promise((r) => setTimeout(r, 1500));
    const pollRes = await fetch(`/api/job/${job_id}`);
    const job: JobStatusResponse = await pollRes.json();

    if (job.status === 'done') {
      return { text_output: job.text_output ?? '', session_id: newSessionId };
    }
    if (job.status === 'error') {
      throw new Error(job.error ?? 'Agent error');
    }
    // 'pending' | 'running' → продолжить polling
  }
}

// ── React: поле ввода с @-триггером ─────────────────────────────────

function ChatInput({ onSend }: { onSend: (text: string, mentions: SegmentMention[]) => void }) {
  const [text, setText] = useState('');
  const [mentions, setMentions] = useState<SegmentMention[]>([]);
  const [suggestions, setSuggestions] = useState<SegmentMention[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);

  const userId = getCurrentUser().email;
  const authHeaders = { 'X-User-Id': userId };

  // Открыть список сегментов по @ или кнопке +
  async function openSegmentPicker() {
    const res = await fetch('/api/segments', { headers: authHeaders });
    const { segments } = await res.json();
    setSuggestions(segments);
    setShowDropdown(true);
  }

  function handleTextChange(value: string) {
    setText(value);
    // Триггер по @ в конце слова
    if (value.endsWith('@') || value.match(/@\w*$/)) {
      openSegmentPicker();
    }
  }

  function selectSegment(segment: SegmentMention) {
    setMentions((prev) => [...prev, segment]);
    // Убрать @ из текста, заменить тегом (визуально)
    setText((prev) => prev.replace(/@\w*$/, ''));
    setShowDropdown(false);
  }

  function removeMention(id: string) {
    setMentions((prev) => prev.filter((m) => m.id !== id));
  }

  function handleSubmit() {
    if (!text.trim() && mentions.length === 0) return;
    onSend(text.trim(), mentions);
    setText('');
    setMentions([]);
  }

  return (
    <div className="chat-input">
      {/* Теги выбранных сегментов */}
      {mentions.map((m) => (
        <span key={m.id} className="segment-tag">
          @{m.name}
          <button onClick={() => removeMention(m.id)}>×</button>
        </span>
      ))}

      <textarea
        value={text}
        onChange={(e) => handleTextChange(e.target.value)}
        placeholder="Задай вопрос... или введите @ для выбора сегмента"
        onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleSubmit()}
      />

      {/* Кнопка + */}
      <button onClick={openSegmentPicker} title="Выбрать сегмент">+</button>
      <button onClick={handleSubmit}>Отправить</button>

      {/* Выпадающий список сегментов */}
      {showDropdown && (
        <ul className="segment-dropdown">
          {suggestions.map((s) => (
            <li key={s.id} onClick={() => selectSegment(s)}>
              <strong>{s.name}</strong>
              <span>~{s.last_count?.toLocaleString()} пользователей</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
```

---

### Итоговый flow

```
Пользователь нажимает + или @
        ↓
GET /api/segments  (X-User-Id)
        ↓
Выбирает "Тёплые лиды Direct"
        ↓
В поле ввода появляется тег [@Тёплые лиды Direct ×]
        ↓
Пользователь добавляет текст и нажимает Отправить
        ↓
buildQueryWithSegments() → query с встроенным SQL сегмента
        ↓
POST /api/analyze  { query, session_id }   ← БЕЗ X-User-Id
        ↓
polling GET /api/job/{job_id}
        ↓
Отображение text_output пользователю
```
