# Скилл: Товарная аналитика

Активируется при вопросах про: товары, продукты, SKU, ассортимент, категории, штуки,
количество, топ товаров, выручка по товарам, позиции в заказе, dm_orders, dm_purchases, dm_products.

---

## Архитектура: три витрины

```
dm_orders    — 1 строка = 1 заказ      (выручка, атрибуция, device, city)
dm_purchases — 1 строка = 1 позиция    (товар, количество, цена)
dm_products  — 1 строка = 1 товар      (агрегат: все метрики + тренды 30 дней)
```

**Связи:**
- `dm_purchases.order_id = dm_orders.order_id`
- `dm_products.product_id = dm_purchases.product_id`

### ⚠️ Критическое правило
**Никогда не делать ARRAY JOIN на таблице `visits` для товарных запросов.**
Таблица visits содержит сырые массивы — прямая работа с ней приводит к зависаниям (851К строк).
Использовать только витрины.

---

## Маршрутизация: какую витрину выбрать

| Вопрос | Витрина |
|--------|---------|
| Топ товаров по выручке / штукам / заказам | `dm_products` (без JOIN, быстро) |
| Растёт или падает конкретный товар (тренд 30д) | `dm_products` |
| Товары, которые давно не продавались | `dm_products` (days_since_last_sale) |
| Ценовой разброс по товару | `dm_products` (min/max/avg_unit_price) |
| Лояльные клиенты товара, повторные покупки | `dm_products` (repeat_buyers, unique_clients) |
| Состав конкретного заказа | `dm_purchases WHERE order_id = '...'` |
| Какие товары покупают через канал X | `dm_purchases JOIN dm_orders ON order_id` |
| Топ товаров по источнику трафика | `dm_purchases JOIN dm_orders ON order_id` |
| Средний чек, выручка, атрибуция без товаров | `dm_orders` (без JOIN) |

**Правило:** `dm_products` — для рейтингов и агрегатов. `dm_purchases` — для детализации и JOIN с `dm_orders`.

---

## Поля dm_orders

Используется когда нужна атрибуция или device/city вместе с товарными данными.

| Поле | Тип | Описание |
|------|-----|----------|
| `order_id` | String | ID заказа — ключ для JOIN с dm_purchases |
| `client_id` | UInt64 | ID клиента |
| `date` | Date | Дата заказа |
| `order_revenue` | Float64 | Точная выручка заказа |
| `utm_source_first` | String | Источник первого визита клиента (first touch) |
| `utm_campaign_first` | String | Кампания первого визита |
| `utm_source_last` | String | Источник в момент покупки (last touch) |
| `utm_campaign_last` | String | Кампания в момент покупки |
| `days_to_purchase` | Int64 | Дней от первого визита до заказа |
| `client_visits_count` | UInt64 | Всего визитов клиента |
| `device` | String | Устройство (desktop / mobile / tablet / tv) |
| `city` | String | Город |

---

## Поля dm_purchases

Детализация на уровне позиции. Всегда в паре с dm_orders если нужна атрибуция.

| Поле | Тип | Описание |
|------|-----|----------|
| `order_id` | String | FK → dm_orders (JOIN ключ) |
| `client_id` | UInt64 | ID клиента |
| `date` | Date | Дата заказа |
| `product_id` | String | ID товара |
| `product_name` | String | Название товара |
| `product_category` | String | Категория товара |
| `quantity` | UInt32 | Точное количество штук на позицию |
| `unit_price` | Float64 | Цена единицы товара |
| `product_revenue` | Float64 | Выручка позиции (unit_price × quantity) |

---

## Поля dm_products

Агрегат — одна строка на товар. Обновляется ежедневно в 04:00.

| Поле | Описание |
|------|----------|
| `product_id` | ID товара — первичный ключ |
| `product_name` | Последнее известное название |
| `product_category` | Последняя известная категория |
| `total_orders` | Уникальных заказов за всё время |
| `total_quantity` | Всего штук продано |
| `total_revenue` | Выручка за всё время |
| `avg_unit_price` | Средняя цена продажи |
| `min_unit_price` | Минимальная цена (нулевые исключены) |
| `max_unit_price` | Максимальная цена |
| `unique_clients` | Уникальных покупателей |
| `repeat_buyers` | Клиенты купившие товар более 1 раза |
| `first_sale_date` | Дата первой продажи |
| `last_sale_date` | Дата последней продажи |
| `days_since_last_sale` | Дней с последней продажи (актуально на дату обновления) |
| `avg_order_value` | Средний чек заказа, когда товар в корзине |
| `pct_solo_orders` | % заказов, где товар был единственным |
| `orders_last_30d` | Заказов за последние 30 дней |
| `revenue_last_30d` | Выручка за последние 30 дней |
| `orders_prev_30d` | Заказов за предыдущие 30 дней (для тренда) |
| `revenue_prev_30d` | Выручка за предыдущие 30 дней (для тренда) |

### Производные метрики (агент считает на лету, не хранятся)
- `repeat_buyers / unique_clients * 100` → % лояльных клиентов
- `(revenue_last_30d - revenue_prev_30d) / revenue_prev_30d * 100` → рост выручки %
- `total_orders / unique_clients` → среднее заказов на клиента
- `(max_unit_price - min_unit_price) / min_unit_price * 100` → ценовой разброс %

---

## Паттерны запросов

### Топ товаров по выручке с трендом
```sql
SELECT
    product_name,
    product_category,
    total_orders,
    total_quantity,
    round(total_revenue)                                              AS total_revenue,
    orders_last_30d,
    round(revenue_last_30d)                                          AS revenue_last_30d,
    round((revenue_last_30d - revenue_prev_30d)
          / nullIf(revenue_prev_30d, 0) * 100, 1)                   AS growth_pct,
    days_since_last_sale
FROM dm_products
ORDER BY total_revenue DESC
LIMIT 20
```

### Товары по источнику трафика (JOIN dm_purchases + dm_orders)
```sql
SELECT
    p.product_name,
    o.utm_source_last                    AS source,
    sum(p.quantity)                      AS qty,
    count(DISTINCT p.order_id)           AS orders,
    round(sum(p.product_revenue))        AS revenue
FROM dm_purchases p
JOIN dm_orders o ON p.order_id = o.order_id
WHERE o.utm_source_last != ''
GROUP BY p.product_name, o.utm_source_last
ORDER BY revenue DESC
LIMIT 20
```

### Состав конкретного заказа
```sql
SELECT
    product_name,
    product_category,
    quantity,
    unit_price,
    round(product_revenue)               AS product_revenue
FROM dm_purchases
WHERE order_id = '12345'
ORDER BY product_revenue DESC
```

### Мёртвые товары (давно не продавались)
```sql
SELECT
    product_name,
    product_category,
    last_sale_date,
    days_since_last_sale,
    total_orders,
    round(total_revenue)                 AS total_revenue
FROM dm_products
WHERE days_since_last_sale > 60
ORDER BY total_revenue DESC
LIMIT 30
```

### Товары с лояльной аудиторией
```sql
SELECT
    product_name,
    unique_clients,
    repeat_buyers,
    round(repeat_buyers / unique_clients * 100, 1)  AS loyalty_pct,
    round(total_revenue)                             AS total_revenue
FROM dm_products
WHERE unique_clients >= 3
ORDER BY loyalty_pct DESC
LIMIT 20
```

### Ценовой разброс (скидки, акции, разные комплектации)
```sql
SELECT
    product_name,
    round(min_unit_price)                AS min_price,
    round(avg_unit_price)                AS avg_price,
    round(max_unit_price)                AS max_price,
    round((max_unit_price - min_unit_price)
          / nullIf(min_unit_price, 0) * 100, 0)     AS spread_pct,
    total_orders
FROM dm_products
WHERE total_orders > 1
ORDER BY spread_pct DESC
LIMIT 20
```

---

## Важные оговорки

### Ценовой разброс — норма
Разброс цен на один `product_id` до 317% — это нормально. Причины: реальные скидки,
изменение прайса со временем, разные комплектации под одним ID в каталоге.

### Анонимные покупки не включены
`dm_orders` и `dm_purchases` фильтруют `clientID > 0`.
Если были анонимные покупки — они не попадут в витрины.

### Период данных
Витрины покрывают данные с 2025-09-01.
Исторические данные до этой даты недоступны.

### dm_products обновляется последней
Расписание: dm_orders 03:00 → dm_purchases 03:30 → dm_products 04:00.
Данные в dm_products актуальны на 04:00 текущего дня.

### Выручка по товарам vs выручка по заказам
`sum(dm_purchases.product_revenue)` ≈ `sum(dm_orders.order_revenue)`.
Незначительная разница (< 0.1%) — погрешность округления Float64.
Для итоговой выручки использовать `dm_orders` как источник истины.
