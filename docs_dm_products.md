# dm_products — Sanok

## Назначение

Витрина товаров: одна строка на `product_id`. Агрегирует **просмотры** (из визитов), **продажи** (из чеков), ценовые метрики, paid/assisted атрибуцию и индикаторы повторных покупок. Основной справочник товарного ассортимента и ценообразования для AI-агента.

Используется в `dm_active_clients_scoring` для расчёта `price_tier` (через `avg_unit_price`).

## Источники данных

| Источник | Что берём |
|----------|----------|
| `visits` | productID[], productName[], productCategory[] → просмотры; DirectClickOrder → paid-метрики |
| `dm_purchases` | order_id, product_id, quantity, unit_price, product_revenue → продажи |
| `dm_orders` | order_revenue → avg_order_value |

## Обновление

**INSERT-triggered MV** (`dm_products_mv`) — пересобирается при каждом INSERT в `visits`.

> **Важно**: поскольку это INSERT-triggered MV, новые батчи вставки добавляют новые строки, а не обновляют существующие. Для получения актуальных агрегатов по product_id нужен `GROUP BY product_id` или использование `argMax` по дате. В будущем рекомендуется миграция на REFRESHABLE MV.

## Масштаб данных

| Метрика | Значение |
|---------|---------|
| Товаров всего | 81 258 |
| С продажами | 5 422 (6.7%) |
| С ценой (avg_unit_price > 0) | 5 422 |
| Период данных | 2025-09-01 — 2026-03-28 |

## Структура таблицы

| Поле | Тип | Описание |
|------|-----|----------|
| `product_id` | String | Уникальный ID товара |
| `product_name` | String | Название товара |
| `product_category` | String | Категория (формат: «Раздел/Подраздел») |
| **Просмотры** | | |
| `total_views` | UInt64 | Общее число просмотров товара |
| `unique_viewers` | UInt64 | Уникальные клиенты, просмотревшие товар |
| `first_view_date` | Date | Дата первого просмотра |
| `last_view_date` | Date | Дата последнего просмотра |
| **Paid-метрики** | | |
| `paid_views` | UInt64 | Просмотры в сессиях с DirectClickOrder > 0 |
| `paid_assisted_views` | UInt64 | Просмотры в органических сессиях, если клиент кликал рекламу в течение 30 дней |
| `paid_purchases` | UInt64 | Покупки в сессиях с DirectClickOrder > 0 |
| `paid_assisted_purchases` | UInt64 | Покупки с assisted-атрибуцией (реклама → 30 дней → органическая покупка) |
| **Продажи** | | |
| `total_orders` | UInt64 | Количество заказов с этим товаром |
| `total_quantity` | UInt64 | Общее число проданных единиц |
| `total_revenue` | Float64 | Общая выручка по товару (руб) |
| `avg_unit_price` | Float64 | Средняя цена за единицу (руб) |
| `min_unit_price` | Float64 | Минимальная цена (исключая нулевые) |
| `max_unit_price` | Float64 | Максимальная цена |
| `unique_clients` | UInt32 | Уникальные покупатели |
| `repeat_buyers` | UInt64 | Покупатели, заказавшие товар более одного раза |
| `first_sale_date` | Date | Дата первой продажи |
| `last_sale_date` | Date | Дата последней продажи |
| `days_since_last_sale` | Int64 | Дней с последней продажи (от today()) |
| `avg_order_value` | Float64 | Средний чек заказов, содержащих этот товар |
| `pct_solo_orders` | Float64 | % заказов, где этот товар был единственным (0–100) |
| **Динамика 30 дней** | | |
| `orders_last_30d` | UInt32 | Заказы за последние 30 дней |
| `revenue_last_30d` | Float64 | Выручка за последние 30 дней |
| `orders_prev_30d` | UInt32 | Заказы за предыдущие 30 дней (30–60 дней назад) |
| `revenue_prev_30d` | Float64 | Выручка за предыдущие 30 дней |
| **Нереализовано** | | |
| `paid_ad_cost` | Float64 | Всегда 0 (источник стоимости кликов удалён из БД) |
| `paid_ad_visits` | UInt32 | Всегда 0 |

## Ценовое распределение (товары с продажами)

| Метрика | Значение |
|---------|---------|
| Минимум | 66 руб |
| P25 | 7 418 руб |
| Медиана | 13 614 руб |
| P75 | 24 485 руб |
| Максимум | 2 234 536 руб |

> Именно эти квантили использованы для `price_tier` в `dm_active_clients_scoring`: low < 7 500, medium 7 500–25 000, high ≥ 25 000.

## ТОП-10 категорий по выручке

| Категория | Товаров | Заказов | Выручка (руб) |
|-----------|---------|---------|---------------|
| Душевые кабины | 3 219 | 370 | 17 668 760 |
| Унитазы с высоким бачком | 26 | 4 | 8 979 654 |
| Встраиваемые раковины | 1 746 | 185 | 8 759 300 |
| Тумбы с раковиной напольные | 4 317 | 356 | 8 206 146 |
| Душевые поддоны | 2 100 | 355 | 6 950 393 |
| Душевые уголки | 3 968 | 250 | 6 912 873 |
| Тумбы с раковиной подвесные | 5 031 | 245 | 6 259 576 |
| Сиденья для унитазов | 846 | 336 | 6 219 314 |
| Унитазы-компакты | 1 190 | 264 | 5 770 422 |
| Акриловые ванны | 2 719 | 149 | 5 684 606 |

## Сценарии использования для AI-агента

### 1. ТОП товаров по выручке

**Триггеры**: "Лучшие товары", "Что продаётся лучше всего?", "ТОП по выручке"

```sql
SELECT
    product_id,
    product_name,
    product_category,
    total_views,
    total_orders,
    round(total_revenue, 0)     AS revenue,
    round(avg_unit_price, 0)    AS avg_price,
    unique_clients
FROM ym_sanok.dm_products
WHERE total_orders > 0
ORDER BY total_revenue DESC
LIMIT 20
```

### 2. Товары с высокой конверсией просмотр→покупка

**Триггеры**: "Какие товары хорошо конвертируют?", "Конверсия просмотров в покупки", "Что смотрят и покупают?"

```sql
SELECT
    product_id,
    product_name,
    product_category,
    total_views,
    unique_viewers,
    total_orders,
    round(total_orders / nullIf(unique_viewers, 0) * 100, 1) AS conv_pct,
    round(avg_unit_price, 0)                                   AS avg_price
FROM ym_sanok.dm_products
WHERE total_orders >= 3
  AND unique_viewers >= 10
ORDER BY conv_pct DESC
LIMIT 20
```

### 3. Эффективность рекламы по товарам

**Триггеры**: "Реклама окупается по товарам?", "Paid vs organic по продуктам", "Какие товары покупают через рекламу?"

```sql
SELECT
    product_id,
    product_name,
    product_category,
    paid_views,
    paid_assisted_views,
    paid_purchases,
    paid_assisted_purchases,
    total_orders,
    round(total_revenue, 0) AS revenue,
    round((paid_purchases + paid_assisted_purchases) / nullIf(total_orders, 0) * 100, 1) AS paid_share_pct
FROM ym_sanok.dm_products
WHERE paid_purchases + paid_assisted_purchases > 0
ORDER BY paid_purchases + paid_assisted_purchases DESC
LIMIT 20
```

### 4. Динамика продаж: текущие 30 дней vs предыдущие

**Триггеры**: "Продажи растут или падают?", "Динамика по товарам за месяц", "Какие товары теряют спрос?"

```sql
SELECT
    product_id,
    product_name,
    orders_last_30d,
    round(revenue_last_30d, 0)  AS rev_30d,
    orders_prev_30d,
    round(revenue_prev_30d, 0)  AS rev_prev_30d,
    round(
        (revenue_last_30d - revenue_prev_30d) / nullIf(revenue_prev_30d, 0) * 100, 1
    ) AS rev_change_pct
FROM ym_sanok.dm_products
WHERE orders_last_30d + orders_prev_30d >= 3
ORDER BY rev_change_pct DESC
LIMIT 20
```

> **Примечание**: 30-дневные окна считаются от `today()`. При устаревших данных оба окна могут быть пустыми.

### 5. Ассортимент по категориям

**Триггеры**: "Сколько товаров в каждой категории?", "Какие категории популярны?", "Ассортимент sanok"

```sql
SELECT
    product_category,
    count()                                           AS products,
    countIf(total_orders > 0)                         AS with_sales,
    sum(total_views)                                  AS views,
    sum(total_orders)                                 AS orders,
    round(sum(total_revenue), 0)                      AS revenue,
    round(avg(avg_unit_price), 0)                     AS avg_price,
    round(sum(total_orders) / nullIf(countIf(total_orders > 0), 0), 1) AS orders_per_product
FROM ym_sanok.dm_products
WHERE product_category != ''
GROUP BY product_category
ORDER BY revenue DESC
LIMIT 20
```

### 6. Товары-кандидаты для ретаргетинга (много просмотров, мало покупок)

**Триггеры**: "Какие товары смотрят но не покупают?", "Проблемные товары", "Высокий интерес — низкая конверсия"

```sql
SELECT
    product_id,
    product_name,
    product_category,
    total_views,
    unique_viewers,
    total_orders,
    round(total_orders / nullIf(unique_viewers, 0) * 100, 1) AS conv_pct,
    round(avg_unit_price, 0)                                   AS avg_price
FROM ym_sanok.dm_products
WHERE unique_viewers >= 50
  AND total_orders <= 1
ORDER BY unique_viewers DESC
LIMIT 20
```

### 7. Средний чек и solo-заказы

**Триггеры**: "Товары, которые покупают отдельно", "Средний чек по товарам", "Кросс-продажи"

```sql
SELECT
    product_id,
    product_name,
    total_orders,
    round(avg_unit_price, 0)   AS avg_price,
    round(avg_order_value, 0)  AS avg_order_val,
    round(pct_solo_orders, 1)  AS solo_pct,
    repeat_buyers
FROM ym_sanok.dm_products
WHERE total_orders >= 5
ORDER BY pct_solo_orders ASC
LIMIT 20
```

> Товары с низким `pct_solo_orders` чаще покупают в паре — это кандидаты для бандлов. Медиана по каталогу = 100% (большинство товаров покупают отдельно).

### 8. Цена товара для scoring (JOIN с dm_active_clients_scoring)

**Триггеры**: "Какие товары смотрят горячие клиенты?", "Ценовой профиль аудитории", "Price tier → товар"

```sql
SELECT
    p.product_id,
    p.product_name,
    round(p.avg_unit_price, 0) AS avg_price,
    count() AS hot_viewers
FROM ym_sanok.dm_active_clients_scoring AS s
CROSS JOIN (
    SELECT clientID, pid
    FROM ym_sanok.visits
    ARRAY JOIN productID AS pid
    WHERE date >= today() - 60
) AS v ON s.client_id = v.clientID
INNER JOIN ym_sanok.dm_products AS p ON v.pid = p.product_id
WHERE s.priority = 'hot'
  AND s.snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
GROUP BY p.product_id, p.product_name, p.avg_unit_price
ORDER BY hot_viewers DESC
LIMIT 20
```

## Технические уточнения

**INSERT-triggered MV**: `dm_products_mv` пересобирает данные при каждом INSERT в `visits`. Это означает, что при множественных инсертах данные могут дублироваться по `product_id`. Для корректных агрегатов лучше группировать по `product_id` в финальном запросе, однако на практике основной backfill был единоразовым.

**paid_ad_cost = 0**: источник стоимости кликов (`master_report_link`) удалён из БД. Поле зарезервировано — при появлении нового источника стоимости обновится.

**avg_unit_price**: рассчитывается только из истории продаж. Товары без продаж имеют `avg_unit_price = 0`. Это влияет на `dm_active_clients_scoring`: клиенты, просматривающие только непроданные товары, получают `price_tier = 'unknown'`.

**pct_solo_orders**: процент заказов, в которых этот товар был единственной позицией. Медиана = 100% — типично для интернет-магазина сантехники (крупные штучные товары).

**Связь с другими таблицами**:
- `dm_active_clients_scoring` → использует `avg_unit_price` для расчёта `price_tier` и `max_viewed_price`
- `dm_purchases` → позиции чека (product_id, quantity, unit_price)
- `dm_orders` → заказ целиком (order_revenue, атрибуция)
- `visits` → сырые данные просмотров (productID[])
