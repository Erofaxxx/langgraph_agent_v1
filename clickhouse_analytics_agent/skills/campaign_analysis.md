# Скилл: Аналитика каналов и атрибуция

Активируется при вопросах про: источники трафика, каналы, кампании, UTM, конверсию,
откуда приходят покупатели, first touch, last touch, путь клиента.

---

## Доступные данные

Данные по **рекламным расходам отсутствуют** — нет витрины с ad spend.
Метрики CPC, CPM, CPA, ROAS рассчитать **невозможно**. Не пытаться их считать.

Доступны: трафик, конверсия, выручка по каналам, атрибуция заказов, пути клиентов.

---

## Какую витрину использовать

| Задача | Витрина |
|--------|---------|
| Трафик по каналу: визиты, отказы, глубина, динамика по дням | `dm_traffic_performance` |
| Конверсия по каналу: CR, покупатели | `dm_campaign_funnel` |
| Выручка и заказы с атрибуцией (first / last touch) | `dm_orders` |
| Полный путь клиента до покупки, мультитач | `dm_conversion_paths` |

Правило приоритета: для выручки по каналу — **всегда `dm_orders`**, не `dm_traffic_performance`.
`dm_traffic_performance.revenue` — сессионная атрибуция, менее точная, подходит только для трендов.

---

## dm_traffic_performance — трафик и динамика

### Поля
| Поле | Описание |
|------|----------|
| `date` | Дата |
| `utm_source` | Источник трафика |
| `utm_medium` | Тип трафика (cpc, organic, email...) |
| `utm_campaign` | Кампания |
| `device` | Устройство (desktop / mobile / tablet) |
| `city` | Город |
| `visits` | Визиты |
| `new_users` | Новые пользователи |
| `bounces` | Отказы |
| `total_duration` | Суммарное время на сайте (секунды) |
| `total_pageviews` | Суммарные просмотры страниц |
| `sessions_with_purchase` | Сессии с покупкой |
| `revenue` | Выручка (сессионная, last touch) |
| `search_engine` | Поисковая система (для органики) |

### Метрики которые можно рассчитать
```sql
-- Трафик по каналу с качеством
SELECT
    utm_source,
    sum(visits)                                          AS visits,
    sum(new_users)                                       AS new_users,
    round(sum(bounces) / sum(visits) * 100, 1)           AS bounce_rate_pct,
    round(sum(total_duration) / sum(visits) / 60, 1)     AS avg_duration_min,
    round(sum(total_pageviews) / sum(visits), 1)         AS avg_pageviews
FROM dm_traffic_performance
WHERE date >= today() - 30
GROUP BY utm_source
ORDER BY visits DESC

-- Динамика визитов по дням
SELECT date, utm_source, sum(visits) AS visits
FROM dm_traffic_performance
WHERE date >= today() - 30
GROUP BY date, utm_source
ORDER BY date, visits DESC
```

---

## dm_campaign_funnel — конверсия по каналу

### Поля
| Поле | Описание |
|------|----------|
| `utm_source` | Источник |
| `utm_campaign` | Кампания |
| `visits` | Всего визитов |
| `sessions_with_purchase` | Сессий с покупкой *(Session track)* |
| `revenue` | Выручка |
| `pre_purchase_visits` | Визитов перед покупкой *(Session track)* |
| `unique_clients_pre_purchase` | Уникальных клиентов до покупки *(Client track)* |
| `unique_buyers` | Уникальных покупателей *(Client track)* |

### ⚠️ КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ — два несовместимых трека

В таблице два трека измерения конверсии. Их **нельзя смешивать ни при каком условии**:

- **Session track:** `visits` → `pre_purchase_visits` → `sessions_with_purchase`
- **Client track:** `unique_clients_pre_purchase` → `unique_buyers`

Делить `unique_buyers` на `visits` или `sessions_with_purchase` на `unique_clients_pre_purchase` — **ошибка**. Результат >100% или бессмысленное число — признак смешения треков.

### Метрики
```sql
-- CR по каналу (Session track)
SELECT
    utm_source,
    sum(visits)                                                      AS visits,
    sum(sessions_with_purchase)                                      AS purchases,
    round(sum(sessions_with_purchase) / sum(visits) * 100, 2)       AS cr_pct,
    sum(revenue)                                                     AS revenue
FROM dm_campaign_funnel
GROUP BY utm_source
ORDER BY revenue DESC

-- CR по клиентам (Client track — отдельно, не смешивать с Session)
SELECT
    utm_source,
    sum(unique_clients_pre_purchase)                                  AS clients_in_funnel,
    sum(unique_buyers)                                                AS buyers,
    round(sum(unique_buyers) / sum(unique_clients_pre_purchase) * 100, 2) AS client_cr_pct
FROM dm_campaign_funnel
GROUP BY utm_source
ORDER BY buyers DESC
```

---

## dm_orders — атрибуция заказов (first touch / last touch)

### Когда использовать вместо других витрин
- Вопрос: "Откуда приходят покупатели?" → **dm_orders**, не dm_traffic_performance
- Вопрос: "Выручка по каналу" → **dm_orders** (точная, per-order), не dm_traffic_performance (сессионная)
- Вопрос: "Сравни first touch и last touch" → **dm_orders** (единственная витрина с обоими)
- Вопрос: "Цикл сделки — сколько дней от первого визита до покупки" → **dm_orders**

### Поля для атрибуции
| Поле | Описание |
|------|----------|
| `order_id` | ID заказа |
| `client_id` | ID клиента |
| `date` | Дата заказа |
| `order_revenue` | Точная выручка заказа |
| `utm_source_first` | Источник **первого** визита клиента (first touch) |
| `utm_campaign_first` | Кампания первого визита |
| `utm_source_last` | Источник визита **в момент покупки** (last touch) |
| `utm_campaign_last` | Кампания в момент покупки |
| `days_to_purchase` | Дней от первого визита до заказа |
| `client_visits_count` | Всего визитов клиента (за всё время) |
| `device` | Устройство в момент покупки |
| `city` | Город покупателя |

### First touch vs Last touch

**Last touch** (`utm_source_last`) — канал, который привёл клиента в момент покупки.
Завышает роль ремаркетинга и брендового трафика.

**First touch** (`utm_source_first`) — канал, с которого клиент узнал о бренде впервые.
Показывает реальный вклад в привлечение новой аудитории.

Для полной картины анализировать оба.

### Типичные запросы

```sql
-- Выручка и заказы по каналу (last touch)
SELECT
    utm_source_last                      AS source,
    count()                              AS orders,
    round(sum(order_revenue))            AS revenue,
    round(avg(order_revenue))            AS avg_check
FROM dm_orders
GROUP BY utm_source_last
ORDER BY revenue DESC

-- Цикл сделки по каналу (first touch)
SELECT
    utm_source_first                     AS source,
    count()                              AS orders,
    round(avg(days_to_purchase))         AS avg_days_to_purchase,
    round(avg(client_visits_count))      AS avg_visits_before_purchase
FROM dm_orders
GROUP BY utm_source_first
ORDER BY orders DESC

-- Матрица first touch vs last touch (атрибуционный расход)
SELECT
    utm_source_first                     AS first_touch,
    utm_source_last                      AS last_touch,
    count()                              AS orders,
    round(sum(order_revenue))            AS revenue
FROM dm_orders
GROUP BY first_touch, last_touch
ORDER BY orders DESC
LIMIT 20

-- Каналы с длинным циклом сделки (потенциал для ремаркетинга)
SELECT
    utm_source_first,
    count()                              AS orders,
    round(avg(days_to_purchase))         AS avg_days,
    round(avg(order_revenue))            AS avg_check
FROM dm_orders
WHERE days_to_purchase > 0
GROUP BY utm_source_first
HAVING orders >= 5
ORDER BY avg_days DESC
```

---

## dm_conversion_paths — полный путь клиента

### Когда использовать
- "Сколько касаний до покупки в среднем?"
- "Какой типичный путь клиента?"
- "Какие последовательности каналов ведут к конверсии?"

### Поля
| Поле | Тип | Описание |
|------|-----|----------|
| `client_id` | UInt64 | ID клиента |
| `converted` | UInt8 | 1 если совершил покупку, 0 если нет |
| `revenue` | Float64 | Выручка клиента |
| `path_length` | UInt16 | Количество касаний (визитов) в пути |
| `first_touch_date` | Date | Дата первого касания |
| `purchase_date` | Date | Дата покупки |
| `conversion_window_days` | UInt16 | Дней от первого касания до покупки |
| `channels_path` | Array(String) | Полный путь по каналам |
| `channels_dedup_path` | Array(String) | Путь без повторов подряд |
| `sources_path` | Array(String) | Путь по источникам (utm_source) |
| `campaigns_path` | Array(String) | Путь по кампаниям |
| `days_from_first_path` | Array(UInt16) | Дней от первого касания на каждом шаге |

### Типичные запросы

```sql
-- Среднее количество касаний до покупки
SELECT
    round(avg(path_length))              AS avg_path_length,
    median(path_length)                  AS median_path_length,
    round(avg(conversion_window_days))   AS avg_days_to_convert
FROM dm_conversion_paths
WHERE converted = 1

-- Топ путей (дедублированных) по частоте
SELECT
    channels_dedup_path                  AS path,
    count()                              AS clients,
    round(sum(revenue))                  AS revenue
FROM dm_conversion_paths
WHERE converted = 1
GROUP BY path
ORDER BY clients DESC
LIMIT 20

-- Распределение по длине пути
SELECT
    path_length,
    count()                              AS clients,
    round(avg(revenue))                  AS avg_revenue
FROM dm_conversion_paths
WHERE converted = 1
GROUP BY path_length
ORDER BY path_length
```

---

## Правила интерпретации

- **Нет расходов** → не считать CPC, CPM, CPA, ROAS. Если пользователь просит ROAS — объяснить, что данных по расходам нет.
- **Малые выборки** → при n < 5 ставить ⚠️ и предупреждать о ненадёжности.
- **Период** → всегда указывать сравниваемые периоды явно (напр. "7–13 марта vs 28 февр.–6 марта").
- **dm_traffic_performance vs dm_orders** → не складывать revenue из обеих таблиц, это один и тот же доход, просто по разной методике атрибуции.
- **Воронка** → не смешивать session track и client track в dm_campaign_funnel (см. предупреждение выше).
