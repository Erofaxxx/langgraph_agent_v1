# Техническое задание: Скилл предзагруженной сегментации
**Проект:** Ask Tab — AI-агент аналитики рекламного трафика
**Версия:** 1.0
**Дата:** 2026-03-09
**Статус:** К разработке

---

## 1. Назначение и контекст

Скилл `segmentation` реализует механизм **предзагруженных именованных сегментов**: маркетолог один раз описывает сегмент в чате, агент формирует его структурное определение, сохраняет в RAG-хранилище, и в дальнейшем обращается к нему по имени — для отчётов, мониторинга, атрибуции и ретаргетинга.

**Ключевой принцип:** сегмент — это **определение (набор условий)**, а не преднарезанная таблица. Агент материализует сегмент динамически через SQL при каждом обращении, что гарантирует актуальность данных.

---

## 2. Доступные данные (схема БД ym_sanok)

Агент работает с 6 витринами и сырой таблицей визитов. Ниже — только те колонки, которые релевантны для сегментации.

### 2.1 `visits` — сырые визиты (851 910 строк)

| Колонка | Тип | Описание |
|---|---|---|
| visitID | UInt64 | ID визита |
| clientID | UInt64 | ID пользователя (Metrica) |
| date | Date | Дата визита |
| dateTime | DateTime | Дата и время визита |
| isNewUser | UInt8 | 1 = новый пользователь |
| bounce | UInt8 | 1 = отказ (bounce) |
| visitDuration | UInt32 | Длительность визита (сек) |
| pageViews | UInt32 | Просмотрено страниц |
| deviceCategory | String | Устройство: desktop/mobile/tablet |
| operatingSystemRoot | String | ОС (windows, android, ios…) |
| browser | String | Браузер |
| regionCity | String | Город пользователя |
| UTMSource | String | utm_source |
| UTMMedium | String | utm_medium |
| UTMCampaign | String | utm_campaign |
| UTMTerm | String | utm_term (поисковая фраза) |
| TrafficSource | String | Источник: ad, organic, direct… |
| SearchEngineRoot | String | Поисковик (yandex, google…) |
| SearchPhrase | String | Поисковый запрос |
| AdvEngine | String | Рекламная система |
| ReferalSource | String | Реферальный источник |
| startURL | String | URL входа |
| goalsID | Array(UInt32) | Достигнутые цели Метрики |
| purchaseID | Array(String) | ID покупок в визите |
| purchaseRevenue | Array(Float64) | Сумма покупок |
| productsCategory | Array(String) | Категории просмотренных товаров |
| impressionsProductID | Array(String) | ID просмотренных товаров |

### 2.2 `dm_client_profile` — профиль клиента (533 352 строки)

> Основная витрина для сегментации по RFM и поведению.

| Колонка | Тип | Описание |
|---|---|---|
| client_id | UInt64 | ID клиента |
| first_visit_date | Date | Дата первого визита (когорта) |
| last_visit_date | Date | Дата последнего визита |
| total_visits | UInt64 | Всего визитов |
| total_pageviews | UInt64 | Всего просмотров страниц |
| total_bounces | UInt64 | Всего отказов |
| total_duration | UInt64 | Суммарное время на сайте (сек) |
| has_purchased | UInt8 | 1 = совершил покупку хоть раз |
| purchase_count | UInt64 | Количество покупок |
| total_revenue | Float64 | Суммарная выручка (LTV) |
| first_utm_source | String | Источник первого визита |
| last_utm_source | String | Источник последнего визита |
| last_utm_campaign | String | Последняя кампания |
| last_device | String | Последнее устройство |
| last_city | String | Последний город |
| days_since_last_visit | UInt16 | Дней с последнего визита (recency) |
| days_first_to_purchase | UInt16 | Дней от первого визита до покупки |
| last_search_engine | String | Последний поисковик |

### 2.3 `dm_client_journey` — путь клиента (851 910 строк)

> Для сегментации по паттернам поведения внутри воронки.

| Колонка | Тип | Описание |
|---|---|---|
| client_id | UInt64 | ID клиента |
| date | Date | Дата визита |
| visit_number | UInt16 | Номер визита клиента |
| total_visits_of_client | UInt16 | Всего визитов клиента на дату |
| utm_source | String | Источник визита |
| utm_medium | String | Тип трафика |
| utm_campaign | String | Кампания |
| device | String | Устройство |
| city | String | Город |
| bounce | UInt8 | Отказ |
| page_views | UInt32 | Просмотры страниц |
| visit_duration | UInt32 | Длительность (сек) |
| has_purchase | UInt8 | 1 = была покупка в этом визите |
| is_last_before_purchase | UInt8 | 1 = последний визит перед покупкой |

### 2.4 `dm_conversion_paths` — пути конверсии (533 352 строки)

> Для атрибуции и сегментации по мультиканальности.

| Колонка | Тип | Описание |
|---|---|---|
| client_id | UInt64 | ID клиента |
| converted | UInt8 | 1 = сконвертировался |
| revenue | Float64 | Выручка |
| path_length | UInt16 | Кол-во касаний до конверсии |
| first_touch_date | Date | Дата первого касания |
| purchase_date | Date | Дата покупки |
| conversion_window_days | UInt16 | Дней от первого касания до покупки |
| channels_path | Array(String) | Последовательность каналов |
| channels_dedup_path | Array(String) | Дедуплицированная последовательность |
| sources_path | Array(String) | Последовательность источников |
| campaigns_path | Array(String) | Последовательность кампаний |
| days_from_first_path | Array(UInt16) | Дни от старта до каждого касания |

### 2.5 `dm_purchases` — покупки (9 815 строк)

| Колонка | Тип | Описание |
|---|---|---|
| client_id | UInt64 | ID клиента |
| date | Date | Дата покупки |
| product_category | String | Категория товара |
| product_name | String | Наименование товара |
| revenue | Float64 | Сумма покупки |
| quantity | UInt16 | Количество |
| utm_source | String | Источник привлечения |
| device | String | Устройство |
| city | String | Город |

### 2.6 `dm_traffic_performance` — трафик (137 243 строки)

> Для сегментации кампаний и каналов по эффективности.

| Колонка | Тип | Описание |
|---|---|---|
| date | Date | Дата |
| utm_source | String | Источник |
| utm_medium | String | Тип |
| utm_campaign | String | Кампания |
| device | String | Устройство |
| city | String | Город |
| visits / new_users / bounces | UInt64 | Трафик |
| revenue | Float64 | Выручка |

---

## 3. Параметры определения сегмента

Ниже — полная схема параметров, которые может задать пользователь. Параметры разбиты на блоки по источнику данных. Все поля **опциональны** кроме `name`.

### 3.1 Системные поля

```json
{
  "segment_id": "string (auto-generated UUID)",
  "name": "string (required) — человекочитаемое имя",
  "description": "string — пояснение от пользователя",
  "created_at": "date (auto)",
  "updated_at": "date (auto)",
  "used_in": ["attribution", "retargeting_report", "weekly_report"]
}
```

### 3.2 Временное окно (ОБЯЗАТЕЛЬНО уточнять у пользователя)

```json
"period": {
  "type": "rolling | fixed | cohort | all_time",

  // rolling: последние N дней от сегодня
  "days": 30,

  // fixed: конкретный диапазон
  "from": "2025-10-01",
  "to": "2025-12-31",

  // cohort: по дате первого события
  "cohort_by": "first_visit | first_purchase",
  "cohort_period": "2025-10"  // YYYY-MM
}
```

> ⚠️ Если тип не указан, агент обязан запросить его у пользователя перед сохранением.

### 3.3 RFM-параметры (из `dm_client_profile`)

```json
"rfm": {
  "recency_days_max": 30,        // days_since_last_visit <= N
  "recency_days_min": null,      // days_since_last_visit >= N

  "frequency_min": 2,            // total_visits >= N
  "frequency_max": null,

  "monetary_min": 5000.0,        // total_revenue >= N
  "monetary_max": null,

  "purchase_count_min": 1,       // purchase_count >= N
  "purchase_count_max": null,

  "has_purchased": true,         // true | false | null (не фильтровать)

  "days_to_purchase_min": null,  // days_first_to_purchase >= N
  "days_to_purchase_max": 7      // быстрые конвертеры
}
```

### 3.4 Источники трафика (из `dm_client_profile`, `dm_client_journey`)

```json
"traffic": {
  // Тип атрибуции: first_touch / last_touch / any_touch
  "attribution_type": "first_touch | last_touch | any_touch",

  "utm_source": ["ya-direct", "google"],      // список допустимых значений
  "utm_medium": ["cpc", "email"],
  "utm_campaign": ["MK_Tovarnaya_Centr"],     // подстрока или точное значение

  "traffic_source": ["ad", "organic", "direct"],
  "search_engine": ["yandex", "google"],

  // Мультиканальность (из dm_conversion_paths)
  "path_contains_channel": "ya-direct",       // канал должен быть в пути
  "path_length_min": 2,                       // минимум касаний
  "path_length_max": null,
  "is_single_channel": false                  // только одноканальные
}
```

### 3.5 Поведение на сайте (из `dm_client_profile`, `visits`)

```json
"behavior": {
  "total_pageviews_min": 3,
  "total_pageviews_max": null,

  "avg_visit_duration_min": 60,   // сек
  "bounce_rate_max": 0.5,          // доля отказов <= 50%

  "viewed_category": ["unitazi", "vanny"],   // из visits.productsCategory
  "goal_reached": [1, 5, 12],               // goalsID Метрики

  "is_new_user": null              // true | false | null
}
```

### 3.6 Гео и устройство (из `dm_client_profile`, `visits`)

```json
"geo_device": {
  "city": ["Москва", "Санкт-Петербург", "Киров"],  // список городов
  "device": ["mobile", "desktop", "tablet"],
  "os": ["android", "ios", "windows"],
  "browser": ["chrome", "yandex_browser"]
}
```

### 3.7 Транзакционные / товарные (из `dm_purchases`)

```json
"purchases": {
  "product_category": ["unitazi", "smesiteli"],
  "avg_order_value_min": 3000.0,
  "avg_order_value_max": null,
  "last_purchase_days_max": 60    // купил в последние N дней
}
```

### 3.8 Воронка / стадия готовности (из `dm_client_journey`)

```json
"funnel": {
  "stage": "visited | viewed_product | near_purchase | converted",
  // visited           = total_visits >= 1
  // viewed_product    = pageviews > 1 AND bounce = 0
  // near_purchase     = is_last_before_purchase = 1 AND has_purchase = 0
  // converted         = has_purchase = 1

  "converted": false,             // дублирует dm_client_profile.has_purchased
  "visits_without_purchase_min": 2  // заходил >= 2 раз, но не купил
}
```

---

## 4. Подходы к сегментации (поддерживаемые скиллом)

Скилл должен поддерживать следующие подходы. Выбор подхода влияет на то, какой SQL-шаблон используется для материализации сегмента.

### 4.1 RFM-сегментация
**Источник:** `dm_client_profile`
**Суть:** разбивка по Recency / Frequency / Monetary
**Когда использовать:** стандартная сегментация покупателей, ретаргетинг, программы лояльности

```sql
-- Пример: "Лояльные покупатели" (R<=30, F>=3, M>=5000)
SELECT client_id
FROM dm_client_profile
WHERE days_since_last_visit <= 30
  AND total_visits >= 3
  AND total_revenue >= 5000
  AND has_purchased = 1
```

### 4.2 Воронко-поведенческая сегментация
**Источник:** `dm_client_journey`, `dm_client_profile`
**Суть:** сегментация по позиции в воронке и паттернам поведения
**Когда использовать:** горячий ретаргетинг (бросили корзину), нагрев верхней воронки

```sql
-- Пример: "Горячий ретаргет" — приходили 2+ раз, не купили
SELECT DISTINCT client_id
FROM dm_client_profile
WHERE total_visits >= 2
  AND has_purchased = 0
  AND days_since_last_visit <= 30
```

### 4.3 Канальная сегментация
**Источник:** `dm_client_profile`, `dm_conversion_paths`
**Суть:** сегментация по источнику первого или последнего касания, или по наличию канала в пути
**Когда использовать:** анализ quality of traffic по каналу, attribution cross-check

```sql
-- Пример: "Пришедшие из paid и не купившие"
SELECT client_id
FROM dm_client_profile
WHERE first_utm_source IN ('ya-direct', 'google')
  AND has_purchased = 0

-- Пример: канал присутствует хоть раз в пути
SELECT client_id
FROM dm_conversion_paths
WHERE has('sources_path', 'ya-direct')
  AND converted = 0
```

### 4.4 Когортная сегментация
**Источник:** `dm_client_profile`
**Суть:** пользователи, совершившие первый визит в определённый период
**Когда использовать:** анализ retention по когортам, оценка кампаний по дате запуска

```sql
-- Пример: когорта октября 2025
SELECT client_id
FROM dm_client_profile
WHERE toYYYYMM(first_visit_date) = 202510
```

### 4.5 Товарная сегментация
**Источник:** `dm_purchases`, `visits`
**Суть:** сегментация по категориям просмотренных или купленных товаров
**Когда использовать:** персонализация рекламы, cross-sell

```sql
-- Пример: смотрели унитазы, не купили
SELECT DISTINCT clientID AS client_id
FROM visits
WHERE has(productsCategory, 'unitazi')
  AND length(purchaseID) = 0

-- Пример: купили из категории ванны
SELECT DISTINCT client_id
FROM dm_purchases
WHERE product_category = 'vanny'
```

### 4.6 Мультиканальная сегментация
**Источник:** `dm_conversion_paths`
**Суть:** сегментация по длине пути, составу каналов, скорости конверсии
**Когда использовать:** attribution, анализ "сложных" покупателей

```sql
-- Пример: мультиканальные конвертеры (3+ касания)
SELECT client_id
FROM dm_conversion_paths
WHERE converted = 1
  AND path_length >= 3
  AND conversion_window_days <= 14
```

---

## 5. JSON-схема хранения в RAG

Полный объект сегмента, который сохраняется в RAG-хранилище:

```json
{
  "segment_id": "seg_7a3f1c",
  "name": "Горячий ретаргет mobile",
  "description": "Пользователи с мобильных, заходили 2+ раз за 30 дней, не купили",
  "approach": "funnel_behavioral",
  "period": {
    "type": "rolling",
    "days": 30
  },
  "conditions": {
    "rfm": {
      "frequency_min": 2,
      "has_purchased": false
    },
    "geo_device": {
      "device": ["mobile"]
    },
    "funnel": {
      "converted": false,
      "visits_without_purchase_min": 2
    }
  },
  "primary_table": "dm_client_profile",
  "join_tables": [],
  "created_at": "2026-03-09",
  "updated_at": "2026-03-09",
  "used_in": ["retargeting_report", "attribution"],
  "last_count": 12430,
  "last_materialized": "2026-03-09"
}
```

---

## 6. Новые файлы для добавления в агент

### 6.1 `skills/segmentation.md`

````markdown
# Segmentation Skill

## Когда активируется
Запросы содержат: "сегмент", "аудитория", "кто из пользователей", "найди пользователей",
"ретаргет", "покупатели которые", "создай сегмент", "сохрани сегмент"

## Два режима работы

### Режим 1: Создание нового сегмента (сохранение в RAG)
Если пользователь хочет сохранить сегмент для повторного использования:
1. Уточни временное окно (если не указано — ОБЯЗАТЕЛЬНО спроси)
2. Сформируй JSON-определение сегмента по схеме из RAG
3. Выбери подход сегментации: rfm | funnel_behavioral | channel | cohort | product | multichannel
4. Покажи пользователю итоговое определение на подтверждение
5. После подтверждения — сохрани в RAG с уникальным segment_id
6. Выполни пробный SQL для подсчёта размера сегмента (last_count)

### Режим 2: Материализация существующего сегмента
Если пользователь ссылается на именованный сегмент:
1. Найди определение в RAG по имени
2. Генерируй SQL из conditions + period
3. Передай результат следующему скиллу (attribution, report и т.д.)

## Правила генерации SQL

### Основная таблица по подходу
- rfm, funnel_behavioral, channel, cohort → dm_client_profile
- multichannel, path → dm_conversion_paths
- product (покупки) → dm_purchases
- product (просмотры) → visits
- детальное поведение → dm_client_journey

### Временное окно
```sql
-- rolling N дней
AND date >= today() - INTERVAL {days} DAY

-- fixed
AND date BETWEEN '{from}' AND '{to}'

-- cohort по первому визиту
AND toYYYYMM(first_visit_date) = {YYYYMM}
```

### Шаблон материализации
```sql
SELECT client_id, count() AS segment_size
FROM {primary_table}
WHERE {conditions}
  AND {period_filter}
```

## Ограничения
- Никогда не создавай сегмент без временного окна (кроме all_time с явным подтверждением)
- Фильтр clientID > 0 обязателен при работе с dm_ml_features
- Для канальной сегментации уточни: first_touch, last_touch или any_touch
- Максимальный результат сегмента без LIMIT — 500K строк

## Формат ответа пользователю
```
**Сегмент: [Название]**
Подход: [тип]
Период: [описание окна]
Условия: [перечисление на русском]
Размер: [N] пользователей

[SQL-запрос в блоке кода для верификации]
```
````

---

### 6.2 Обновление `skills/_registry.py`

```python
SKILLS = {
    # ... существующие скиллы ...

    "segmentation": {
        "description": (
            "User segmentation: creating named segments by RFM, funnel stage, "
            "channel attribution, cohort, product interest, or multichannel path. "
            "Use when user wants to define, save, or retrieve audience segments "
            "for retargeting, attribution, or reporting."
        ),
        "router_hints": [
            "сегмент", "аудитория", "ретаргет", "пользователи которые",
            "найди клиентов", "создай сегмент", "сохрани сегмент",
            "кто покупал", "кто смотрел", "лояльные", "новые пользователи",
            "segment", "audience", "cohort", "rfm"
        ],
        "file": "skills/segmentation.md",
        "compatible_with": ["attribution", "python_analysis", "visualization", "weekly_report"]
    },
}
```

---

### 6.3 Обновление `requirements.txt`

```
# Добавить для атрибуции (следующий скилл):
networkx>=3.0        # Марковские цепи в атрибуции
```

---

## 7. Диалоговый сценарий (UX-flow)

```
Пользователь:
  "Хочу следить за сегментом: те, кто пришёл из яндекс директа,
   заходил 2+ раза, но ничего не купил. Назови 'Тёплые лиды Direct'"

Агент (шаг 1 — уточнение):
  "За какой период смотреть? Рекомендую: последние 30 дней.
   Или укажи конкретные даты."

Пользователь:
  "Последние 30 дней"

Агент (шаг 2 — показывает определение):
  "Создаю сегмент **Тёплые лиды Direct**:
   - Период: последние 30 дней
   - Источник первого касания: ya-direct
   - Визитов: ≥ 2
   - Покупок: 0

   Предварительный подсчёт: ~8 240 пользователей

   Сохранить сегмент?"

Пользователь:
  "Да"

Агент (шаг 3 — сохранение):
  "Сегмент сохранён. Теперь вы можете запросить:
   - 'Покажи атрибуцию для Тёплых лидов Direct'
   - 'Как изменился размер Тёплых лидов Direct за месяц'
   - 'Включи Тёплых лидов Direct в еженедельный отчёт'"
```

---

## 8. Связь с атрибуционным скиллом

Сегментация служит **входными данными** для attribution. Порядок работы:

```
Router детектирует: "атрибуция для сегмента X"
  → активирует скиллы: ["segmentation", "attribution"]

Agent:
  1. segmentation skill → находит сегмент X в RAG → генерирует список client_id
  2. attribution skill  → фильтрует dm_conversion_paths по client_id из шага 1
  3. Считает Markov / Shapley только по этой выборке
  4. Возвращает результат: "вклад каналов для сегмента X"
```

Это позволяет отвечать на вопросы типа:
> "Для лояльных покупателей какой канал инициирует спрос, а какой закрывает сделку?"

---

## 9. Приоритет разработки

| Этап | Задача | Зависимости |
|---|---|---|
| 1 | Добавить `skills/segmentation.md` | — |
| 2 | Обновить `skills/_registry.py` | Этап 1 |
| 3 | Реализовать сохранение в RAG (когда RAG готов) | RAG-модуль |
| 4 | Временная заглушка: сегменты в SQLite session store | Этапы 1-2 |
| 5 | Добавить `skills/attribution.md` + networkx | Этапы 1-2 |
| 6 | Интеграция attribution × segmentation | Этапы 1-5 |

**До появления RAG (этап 3):** сегменты хранить в существующей SQLite-базе (`chat_history.db`) отдельной таблицей `segments` с теми же JSON-полями. При переходе на RAG — миграция без изменения логики агента.

---

## 10. Возможные расширения (backlog)

- **ML-кластеризация** через `python_analysis` скилл: K-Means по фичам из `dm_client_profile` + `dm_ml_features` с автоматическим именованием кластеров через LLM
- **Динамические сегменты**: пересчёт размера сегмента при каждом запросе с отображением тренда
- **Пересечение сегментов**: "покажи пересечение 'Тёплых лидов Direct' и 'мобильных пользователей'"
- **Экспорт сегмента**: список client_id для загрузки в рекламный кабинет
