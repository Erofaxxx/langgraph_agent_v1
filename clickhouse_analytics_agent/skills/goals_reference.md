## Skill: Справочник целей Яндекс Метрики (Sanok)

Используется при любых вопросах про цели, goal_*, конверсии, заказы, события на сайте.

Цели хранятся в dm_direct_performance (агрегированные колонки) и в visits.goalsID (Array(UInt32)).

---

## Ключевые цели для аналитики

### Ecommerce-воронка (главные KPI)

| ID | Название | Колонка в dm_direct_performance | Тип |
|----|----------|--------------------------------|-----|
| 3000178943 | **Ecommerce: покупка** | `order_paid` | **ФИНАЛЬНЫЙ KPI** |
| 31297300 | Заказ оформлен | `order_created` | Лид |
| 21115915 | Оформление заказа | `checkout_started` | Микроконверсия |
| 194388760 | Добавление в корзину | `add_to_cart` | Микроконверсия |
| 21115645 | Корзина | `cart_visits` | Микроконверсия |
| 543662405 | Просмотр товара | `product_views` | Вовлечённость |
| 201398152 | Уникальный звонок | `unique_calls` | Лид |

### Новые DataLayer-цели (серия 543662xxx, с 28.03.2026)

| ID | Название | Идентификатор |
|----|----------|---------------|
| 543662393 | Успешное оформление заказа | order_prepaid + order_postpaid |
| 543662395 | Заказ с предоплатой | order_prepaid |
| 543662396 | Заказ с постоплатой | order_postpaid |
| 543662401 | Отмененный заказ | order_cancelled |
| 543662402 | Заказ доставлен | order_delivered |
| 543662403 | Добавить в корзину | add_to_cart |
| 543662404 | Удалить из корзины | remove_from_cart |
| 543662405 | Просмотр товара | view_product_details |
| 543662406 | Клик по товару | product_click |
| 543662407 | Просмотр списка товаров | product_list_view |
| 543662408 | Переход на чекаут | checkout_open |

> ⚠️ Серия 543662xxx — новые цели, данные в direct_custom_report пока нулевые (колонки созданы, данные придут при следующих выгрузках).

### Звонки (Calltouch)

| ID | Название | Тип |
|----|----------|-----|
| 34740969 | Звонок Calltouch | Все звонки |
| 201398152 | Уникальный звонок | Лид (первичный) |
| 201398155 | Уникально-целевой звонок | Лид (≥30 сек) |
| 201398158 | Целевой звонок | Лид (≥30 сек) |

### URL-цели воронки

| ID | Название | URL |
|----|----------|-----|
| 21115645 | Корзина | /cart |
| 21115915 | Оформление заказа | /simplecheckout |
| 21115920 | Контакты | kontakti/ |
| 21115925 | Акции | akcii/ |

### Jivo Chat

| ID | Название |
|----|----------|
| 47284792 | Jivo: установлен диалог |
| 47284804 | Jivo: чат запрошен клиентом |
| 47284867 | Jivo: чат начат клиентом |
| 47284936 | Jivo: клиент принял приглашение |
| 47284972 | Jivo: заполнил форму контактов |
| 47285017 | Jivo: офлайн-сообщение |

### Автоцели Метрики

| ID | Название |
|----|----------|
| 175941400 | Клик по телефону |
| 175941403 | Клик по email |
| 175941406 | Поиск по сайту |
| 175941409 | Отправка формы |
| 195990724 | Переход в соцсеть |
| 211567495 | Скачивание файла |
| 232782778 | Переход в мессенджер |
| 317172960 | Заполнил контактные данные |
| 339159404 | Отправил контактные данные |
| 495725161 | Начало оформления заказа |

### Прочие

| ID | Название | Примечание |
|----|----------|------------|
| 130300219 | Посетили сайт | Шумовая цель |
| 157742788 | Более 3-х страниц | Вовлечённость |

---

## Воронка целей

```
Визит
  └─ Вовлечённо��ть: просмотр товара (543662405), поиск по сайту (175941406)
       └─ Микроконверсия: корзина (21115645 / 194388760), чекаут (21115915)
            └─ Звонок: 201398152 (уникальный), 201398155 (уникально-целевой)
                 └─ Заказ оформлен (31297300) → order_created
                      └─ **Ecommerce: покупка (3000178943)** → order_paid ← ФИНАЛЬНЫЙ KPI
```

## Таблица goal_dict — словарь целей (65 целей)

Справочник `goal_id` → `goal_name` + `goal_category` + `goal_trigger`. Статическая таблица, обновляется вручную.

**Структура:** `goal_id` (UInt64), `goal_name` (String), `goal_category` (LowCardinality), `goal_trigger` (String).

### Категории целей

| Категория | Кол-во | Описание |
|-----------|--------|----------|
| `транзакция` | 7 | Покупка, заказ, предоплата/постоплата, доставка, отмена |
| `воронка` | 12 | Просмотр товара → корзина → чекаут |
| `звонки` | 4 | Calltouch: уникальные, целевые |
| `категория` | 19 | Категорийные страницы (ванны, унитазы, смесители) |
| `jivo` | 7 | Онлайн-чат Jivo |
| `автоцель` | 10 | Клик по телефону, email, формы, поиск |
| `прочее` | 6 | Контакты, акции, шумовые |

### Где goal_id используется в других таблицах

| Таблица | Поле | Тип JOIN |
|---------|------|----------|
| `campaigns_settings` | `strategy_search_goal_id` | `ON s.strategy_search_goal_id = toInt64(g.goal_id)` |
| `campaigns_settings` | `priority_goal_ids` | `arrayJoin` + JOIN |
| `dm_step_goal_impact` | `goal_id` | Прямой JOIN |
| `dm_active_clients_scoring` | `recommended_goal_id` | Прямой JOIN |
| `socdem_direct_analytics` | `MacroConversions` | Сумма 9 целей (см. ниже) |

### SQL-шаблоны

```sql
-- Расшифровать конкретный goal_id
SELECT goal_id, goal_name, goal_category, goal_trigger
FROM ym_sanok.goal_dict WHERE goal_id = 3000178943;

-- Все цели категории
SELECT goal_id, goal_name, goal_trigger
FROM ym_sanok.goal_dict WHERE goal_category = 'воронка' ORDER BY goal_id;

-- Цели автостратегии кампании
SELECT s.campaign_name, s.strategy_search_goal_id,
       g.goal_name, g.goal_category
FROM ym_sanok.campaigns_settings FINAL s
LEFT JOIN ym_sanok.goal_dict g ON s.strategy_search_goal_id = toInt64(g.goal_id)
WHERE s.state = 'ON' AND s.strategy_search_goal_id IS NOT NULL;

-- Приоритетные цели кампании с расшифровкой
SELECT s.campaign_name, gid AS goal_id, g.goal_name, gval AS goal_value
FROM ym_sanok.campaigns_settings FINAL s
ARRAY JOIN priority_goal_ids AS gid, priority_goal_values AS gval
LEFT JOIN ym_sanok.goal_dict g ON gid = toInt64(g.goal_id)
WHERE length(s.priority_goal_ids) > 0
ORDER BY s.campaign_id, gval DESC;
```

---

## Готовые агрегаты для SQL (dm_direct_performance)

```sql
-- Ecommerce-воронка:
SUM(product_views) AS views,
SUM(add_to_cart) AS add_cart,
SUM(cart_visits) AS cart,
SUM(checkout_started) AS checkout,
SUM(order_created) AS orders,
SUM(order_paid) AS paid

-- ROAS:
round(SUM(purchase_revenue) / nullIf(SUM(cost), 0), 2) AS roas

-- CR в покупку от кликов:
round(SUM(order_paid) / nullIf(SUM(clicks), 0) * 100, 2) AS paid_cr_pct
```
