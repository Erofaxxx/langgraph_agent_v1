# Скилл: Гео-анализ эффективности рекламы

Активируется при вопросах про: регион, город, гео, география, геотаргетинг,
по регионам, по городам, территория, где покупают, какие города эффективны,
региональная эффективность, ROAS по городам, расходы по регионам.

---

## Источники данных и покрытие

Города в Директе — **на русском** (LocationOfPresenceName).
Города в Метрике — **на английском** (поле city в visits).
Для объединения используются словари `geo_city_map` и `dict_geo_ru_to_en`.

**Покрытие перевода: ~90%** — 10% регионов не имеют сопоставления и остаются как `region IS NULL` в dm_direct_by_geo. Это нормально, учитывать в анализе.

---

## Какую таблицу использовать

| Задача | Таблица |
|--------|---------|
| ROAS, CPS, заказы по регионам (интеграция с Метрикой) | `dm_direct_by_geo` |
| Ecommerce-воронка по регионам (order_paid, cart_visits, add_to_cart) | `dm_direct_by_region` |
| Расходы, CTR, клики по городам (только Директ, на русском) | `direct_campaigns` GROUP BY LocationOfPresenceName |
| Перевод русских → английских названий городов | `geo_city_map` / `dict_geo_ru_to_en` |

---

## Поля dm_direct_by_geo

| Поле | Тип | Описание |
|------|-----|----------|
| `Date` | Date | Дата |
| `CampaignId` | UInt64 | ID кампании |
| `region` | Nullable(String) | Регион на английском (NULL если словарь не покрыл) |
| `campaign_name` | String | Название кампании |
| `campaign_type` | String | Тип кампании |
| `impressions` | UInt64 | Показы |
| `clicks` | UInt64 | Клики |
| `cost` | Float64 | Расходы (руб.) |
| `sessions` | UInt64 | Сессии из Метрики |
| `conversions` | UInt64 | Конверсии |
| `orders_lastclick` | UInt64 | Заказы last-click |
| `revenue_lastclick` | Float64 | Выручка last-click |
| `avg_cpc` | Float64 | Средняя цена клика |
| `bounce_rate` | Float64 | Показатель отказов (%) |
| `avg_pageviews` | Float64 | Среднее число страниц |
| `conversion_rate` | Float64 | CR (%) |
| `orders_later_adj` | Float64 | Заказы с ассистированием |
| `later_revenue_adj` | Float64 | Выручка ассистированных (30%) |
| `later_converters` | UInt64 | Уникальных ассистирующих клиентов |
| `total_orders` | Float64 | Итого заказов |
| `total_attr_revenue` | Float64 | Итого атрибутированная выручка |
| `roas_lastclick` | Nullable(Float64) | ROAS консервативный |
| `roas_attributed` | Nullable(Float64) | ROAS с ассистированием |
| `cps` | Nullable(Float64) | Стоимость заказа |

---

## SQL-паттерны

### Топ регионов по ROAS
```sql
SELECT
    region,
    sum(cost)                                                     AS total_cost,
    sum(clicks)                                                   AS total_clicks,
    sum(total_orders)                                             AS total_orders,
    round(sum(total_attr_revenue))                                AS attr_revenue,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas,
    round(sum(cost) / nullIf(sum(total_orders), 0))              AS cps
FROM dm_direct_by_geo
WHERE Date >= today() - 30
    AND region IS NOT NULL
GROUP BY region
HAVING total_cost > 0
ORDER BY roas DESC
LIMIT 20
```

### Расходы и эффективность по регионам с учётом непокрытых
```sql
SELECT
    coalesce(region, '[регион не определён]')                    AS region,
    sum(cost)                                                     AS cost,
    sum(total_orders)                                             AS orders,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas
FROM dm_direct_by_geo
WHERE Date >= today() - 30
GROUP BY region
HAVING cost > 0
ORDER BY cost DESC
LIMIT 30
```

### Сравнение регионов по эффективности (только с заказами)
```sql
SELECT
    region,
    sum(cost)                                                     AS cost,
    sum(total_orders)                                             AS orders,
    round(sum(total_attr_revenue))                                AS revenue,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas,
    round(sum(cost) / nullIf(sum(total_orders), 0))              AS cps,
    round(sum(clicks) / nullIf(sum(impressions), 0) * 100, 2)   AS ctr_pct,
    round(sum(bounce_rate * sessions) / nullIf(sum(sessions), 0), 1) AS avg_bounce_pct
FROM dm_direct_by_geo
WHERE Date >= today() - 30
    AND region IS NOT NULL
GROUP BY region
HAVING orders > 0
ORDER BY orders DESC
LIMIT 30
```

### Расходы по городам из сырых данных Директа (на русском)
```sql
SELECT
    LocationOfPresenceName                                        AS city_ru,
    sum(Cost)                                                     AS cost,
    sum(Clicks)                                                   AS clicks,
    sum(Impressions)                                              AS impressions,
    round(sum(Clicks) / nullIf(sum(Impressions), 0) * 100, 2)   AS ctr_pct,
    round(sum(Cost) / nullIf(sum(Clicks), 0), 2)                AS avg_cpc
FROM direct_campaigns
WHERE Date >= today() - 30
    AND LocationOfPresenceName != ''
GROUP BY LocationOfPresenceName
ORDER BY cost DESC
LIMIT 30
```

### Сравнение ROAS: Москва vs регионы
```sql
SELECT
    multiIf(
        region IN ('Moscow', 'Moskovskaya Oblast', 'Moscow Oblast'), 'Москва и МО',
        region IS NULL, 'Регион не определён',
        'Регионы'
    )                                                             AS geo_group,
    sum(cost)                                                     AS cost,
    sum(total_orders)                                             AS orders,
    round(sum(total_attr_revenue))                                AS revenue,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas,
    round(sum(cost) / nullIf(sum(total_orders), 0))              AS cps
FROM dm_direct_by_geo
WHERE Date >= today() - 30
GROUP BY geo_group
ORDER BY cost DESC
```

### Динамика ROAS по топ-регионам
```sql
SELECT
    Date,
    region,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas,
    sum(cost)                                                     AS cost
FROM dm_direct_by_geo
WHERE Date >= today() - 30
    AND region IN (
        SELECT region
        FROM dm_direct_by_geo
        WHERE Date >= today() - 30 AND region IS NOT NULL
        GROUP BY region
        ORDER BY sum(cost) DESC
        LIMIT 5
    )
GROUP BY Date, region
ORDER BY Date, cost DESC
```

---

## dm_direct_by_region — Директ по регионам (ecommerce-воронка)

Витрина с региональной гранулярностью из Direct API. Дополнение к `dm_direct_by_geo` — те же метрики, что в `dm_direct_performance`, плюс `location_id` / `location_name`.

> `location_name` — регион **фактического нахождения** пользователя в момент клика, не регион таргетинга.

**Движок:** MergeTree (не ReplacingMergeTree — `FINAL` не нужен).
**История:** с 2025-09-01.

### Поля dm_direct_by_region

| Поле | Тип | Описание |
|------|-----|----------|
| `date` | Date | Дата |
| `campaign_id` | UInt64 | ID кампании |
| `campaign_name` | String | Название |
| `adgroup_id` | UInt64 | ID группы |
| `adgroup_name` | String | Название группы |
| `ad_network_type` | String | SEARCH / AD_NETWORK |
| `location_id` | UInt64 | Яндекс geo ID |
| `location_name` | String | Название региона (русский) |
| `impressions` / `clicks` / `cost` | | Трафик |
| `purchase_revenue` / `purchase_profit` | Float64 | Выручка / прибыль |
| `cart_visits` / `product_views` / `add_to_cart` | UInt64 | Ecommerce-воронка |
| `checkout_started` / `order_created` / `order_paid` | UInt64 | Воронка заказов |
| `unique_calls` | UInt64 | Уникальные звонки |

### Когда dm_direct_by_region лучше dm_direct_by_geo

- Нужна **ecommerce-воронка** (add_to_cart, checkout_started, order_paid) в разрезе регионов
- Нужен `adgroup_id` / `ad_network_type` в разрезе регионов
- Названия регионов на русском (не нужен перевод через geo_city_map)

### SQL-паттерны dm_direct_by_region

```sql
-- Топ регионов по расходам с воронкой
SELECT location_name,
       sum(cost) AS cost, sum(clicks) AS clicks,
       sum(add_to_cart) AS atc, sum(checkout_started) AS checkout,
       sum(order_paid) AS orders, sum(purchase_revenue) AS revenue
FROM ym_sanok.dm_direct_by_region
WHERE date >= today() - 30
GROUP BY location_name
ORDER BY cost DESC LIMIT 20;

-- ROAS по регионам
SELECT location_name,
       sum(purchase_revenue) / nullIf(sum(cost), 0) AS roas,
       sum(order_paid) AS orders, sum(cost) AS cost
FROM ym_sanok.dm_direct_by_region
WHERE date >= today() - 30
GROUP BY location_name
ORDER BY roas DESC;

-- Сравнение кампаний внутри региона
SELECT campaign_name, sum(cost) AS cost, sum(order_paid) AS orders
FROM ym_sanok.dm_direct_by_region
WHERE location_name = 'Москва' AND date >= today() - 30
GROUP BY campaign_name ORDER BY cost DESC;
```

---

## Справка по словарям

### geo_city_map
| Поле | Описание |
|------|----------|
| `ru_name` | Название города на русском (как в Директе) |
| `en_name` | Название на английском (как в Метрике) |
| `notes` | Примечания (населённые пункты, посёлки) |

Используется для разовой проверки или ручного джойна:
```sql
-- Проверить перевод конкретного города
SELECT ru_name, en_name, notes
FROM geo_city_map
WHERE ru_name LIKE '%Москва%'
LIMIT 10
```

---

## Правила интерпретации

- **region IS NULL** — 10% трафика с непокрытыми городами. При агрегации учитывать или явно фильтровать
- **Малые регионы (cost < 1000 ₽)** → статистически ненадёжный ROAS, ставить ⚠️
- **nullIf(sum(cost), 0)** — всегда защищать деление при расчёте ROAS и CPS
- **Москва и МО** — основной рынок, ожидаемо высокая доля бюджета, не всегда лучший ROAS
- **Регион vs кампания**: разные кампании таргетированы на разные регионы — сравнивать регионы корректно только внутри одной кампании или одного типа кампаний
- **LocationOfPresenceName** в direct_campaigns — место нахождения пользователя, не таргетинга. Могут быть показы за пределами целевого региона кампании.
- **Всегда указывать** долю непокрытых регионов (`region IS NULL`) в общих расходах при гео-отчётах
