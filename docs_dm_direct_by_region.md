# dm_direct_by_region — Директ по регионам

**База:** `ym_sanok`  
**Таблица:** `dm_direct_by_region`  
**MV:** `dm_direct_by_region_mv`  
**Движок:** MergeTree, ORDER BY (date, campaign_id, location_id)  
**Источник:** `ym_sanok.direct_custom_report` (Яндекс Директ API)  
**История:** с 2025-09-01

## Назначение

Витрина для анализа эффективности рекламных кампаний в разрезе регионов присутствия пользователей (`LocationOfPresence`). Является дополнением к `dm_direct_performance` — метрики те же, но добавлена региональная гранулярность.

> `location_name` — регион **фактического нахождения** пользователя в момент клика, не регион таргетинга кампании.

## Колонки

| Колонка | Тип | Описание |
|---|---|---|
| `date` | Date | Дата |
| `campaign_id` | UInt64 | ID кампании |
| `campaign_name` | String | Название кампании |
| `adgroup_id` | UInt64 | ID группы объявлений |
| `adgroup_name` | String | Название группы |
| `ad_network_type` | String | Тип сети (SEARCH / AD_NETWORK) |
| `location_id` | UInt64 | ID региона (Яндекс geo ID) |
| `location_name` | String | Название региона |
| `impressions` | UInt64 | Показы |
| `clicks` | UInt64 | Клики |
| `cost` | Float64 | Расходы, руб. |
| `sessions` | UInt64 | Сессии |
| `bounces` | UInt64 | Отказы |
| `purchase_revenue` | Float64 | Выручка с покупок |
| `purchase_profit` | Float64 | Прибыль с покупок |
| `cart_visits` | UInt64 | Визиты с просмотром корзины |
| `product_views` | UInt64 | Просмотры карточек товаров |
| `add_to_cart` | UInt64 | Добавления в корзину |
| `checkout_started` | UInt64 | Начало оформления заказа |
| `order_created` | UInt64 | Заказы созданы |
| `order_paid` | UInt64 | Заказы оплачены |
| `unique_calls` | UInt64 | Уникальные звонки |
| `_ins_ts` | DateTime | Время вставки строки |

## Примеры запросов

**Топ регионов по расходам:**
```sql
SELECT
    location_name,
    sum(cost)             AS total_cost,
    sum(clicks)           AS total_clicks,
    sum(order_paid)       AS orders,
    sum(purchase_revenue) AS revenue
FROM ym_sanok.dm_direct_by_region
WHERE date >= today() - 30
GROUP BY location_name
ORDER BY total_cost DESC
LIMIT 20;
```

**ROI по регионам:**
```sql
SELECT
    location_name,
    sum(purchase_revenue) / nullIf(sum(cost), 0) AS roas,
    sum(order_paid)                               AS orders,
    sum(cost)                                     AS cost
FROM ym_sanok.dm_direct_by_region
WHERE date >= today() - 30
GROUP BY location_name
ORDER BY roas DESC;
```

**Сравнение кампаний внутри региона:**
```sql
SELECT
    campaign_name,
    sum(cost)       AS cost,
    sum(order_paid) AS orders,
    sum(clicks)     AS clicks
FROM ym_sanok.dm_direct_by_region
WHERE location_name = 'Москва'
  AND date >= today() - 30
GROUP BY campaign_name
ORDER BY cost DESC;
```
