# ym_sanok.dm_direct_performance

## Назначение

Главная витрина статистики Яндекс Директа для sanok — интернет-магазина сантехники. Ежедневный срез по каждой паре кампания × группа × тип сети. Охватывает:
- трафик (показы, клики, сессии, отказы);
- расход и выручку (атрибуция Директа);
- ecommerce-воронку: просмотр товара → корзина → чекаут → заказ создан → заказ оплачен.

Это первое место, куда смотрит агент при любом вопросе о результатах рекламы sanok.

## Источник данных и обновление

- Источник: `ym_sanok.direct_custom_report` (кастомный отчёт Я.Директа, атрибуция LSCCD)
- **Тип MV**: INSERT-triggered (срабатывает автоматически при каждом INSERT в источник)
- **Движок target**: `ReplacingMergeTree(_ins_ts)`
- **Ключ дедупа**: `ORDER BY (date, campaign_id, adgroup_id, ad_network_type)`
- **Партицирование**: `PARTITION BY toYYYYMM(date)`
- **Версия**: `_ins_ts DateTime` = `now()` в MV-SELECT. Все строки одного INSERT получают одинаковый `_ins_ts`. При перезаливе дня источник передаёт в MV новые строки → новый `_ins_ts` → `ReplacingMergeTree` при мёрдже оставляет только последнюю версию строки с тем же ключом.

### Почему ReplacingMergeTree, а не SummingMergeTree

`SummingMergeTree` суммировал бы повторные батчи и давал бы задвоение при перезаливе одной даты. `ReplacingMergeTree(_ins_ts)` корректно обрабатывает и полный перезалив, и частичный (по одному дню). Побочный эффект — запросы **должны** использовать `FINAL` либо `argMax` для точных чисел:

```sql
SELECT ... FROM ym_sanok.dm_direct_performance FINAL WHERE ...
```

## Гранулярность

Одна строка = **день × кампания × группа × тип сети (SEARCH / AD_NETWORK)**.

- Для уровня кампании — `GROUP BY campaign_id`
- Для уровня дня — `GROUP BY date`
- Для сравнения поиска и РСЯ — `GROUP BY ad_network_type`

## Покрытие (на момент бэкфилла)

- Период: **2025-09-01 — 2026-03-28** (≈ 7 месяцев)
- Строк после дедупа: **8 062**
- Кампаний: **28**
- Групп: **133**
- Сумма кликов: **216 533**
- Сумма расхода: **4 847 580 руб**
- Сумма выручки: **368 122 833 руб**
- Оплаченных заказов: **1 136**

### Поиск vs РСЯ (весь период)

| | SEARCH | AD_NETWORK |
|--|--------|------------|
| Клики | 102 519 | 114 014 |
| Расход | 3 703 866 ₽ | 1 143 713 ₽ |
| Выручка | 249 509 319 ₽ | 118 613 514 ₽ |
| Заказов оплачено | 969 | 167 |
| CPC (средний) | ≈ 36 ₽ | ≈ 10 ₽ |
| ROMI | ×67 | ×104 |

## Структура таблицы

### Идентификация
| Поле | Тип | Описание |
|------|-----|----------|
| date | Date | Дата (день) |
| campaign_id | UInt64 | ID кампании — JOIN к `campaigns_settings` |
| campaign_name | String | Название кампании (снепшот на момент отчёта) |
| adgroup_id | UInt64 | ID группы — JOIN к `adgroups_settings.group_id` |
| adgroup_name | String | Название группы |
| ad_network_type | String | `SEARCH` или `AD_NETWORK` (РСЯ) |

### Трафик
| Поле | Тип | Описание |
|------|-----|----------|
| impressions | UInt64 | Показы |
| clicks | UInt64 | Клики |
| cost | Float64 | Расход, руб (с НДС) |
| sessions | UInt64 | Сессии в Метрике |
| bounces | UInt64 | Отказы Метрики |

### Выручка (атрибуция Директа, LSCCD)
| Поле | Тип | Описание |
|------|-----|----------|
| purchase_revenue | Float64 | Выручка по покупкам |
| purchase_profit | Float64 | Прибыль (если считается на стороне Метрики) |

### Ecommerce-воронка sanok
| Поле | Тип | goal_id | Описание |
|------|-----|---------|----------|
| cart_visits | UInt64 | `21115645` | Посещение страницы корзины (URL /cart) |
| product_views | UInt64 | `543662405` | Просмотр карточки товара (DataLayer: view_product_details) — нулевой до 28.03.2026, наполняется с новых выгрузок |
| add_to_cart | UInt64 | `194388760` | Ecommerce: добавление в корзину |
| checkout_started | UInt64 | `21115915` | Оформление заказа (URL /simplecheckout) |
| order_created | UInt64 | `31297300` | Заказ оформлен (URL /checkout/success) |
| order_paid | UInt64 | `3000178943` | Ecommerce: покупка (100% коррелирует с `purchaseID` в visits) |

### Звонки
| Поле | Тип | goal_id | Описание |
|------|-----|---------|----------|
| unique_calls | UInt64 | `201398152` | Уникальный звонок (Calltouch) |

### Позиции показов
| Поле | Тип | Описание |
|------|-----|----------|
| impressions_premium | UInt64 | Показы в спецразмещении (Slot = PREMIUMBLOCK) |
| impressions_other | UInt64 | Показы в гарантии (Slot = OTHER) |
| clicks_premium | UInt64 | Клики из спецразмещения |
| clicks_other | UInt64 | Клики из гарантии |
| avg_impression_position | Float64 | Средняя позиция показа (взвешенная по показам) |
| avg_click_position | Float64 | Средняя позиция клика (взвешенная по кликам) |

> Остальные слоты из источника (`PRODUCT_GALLERY`, `ALONE`, `SUGGEST`, `CPA_NETWORK`) в колонки не вынесены — при необходимости доступны через `direct_custom_report`.

### Служебное
| Поле | Тип | Описание |
|------|-----|----------|
| _ins_ts | DateTime | Версия батча для ReplacingMergeTree |

## Соответствие goal_id → шаг воронки

Маппинг сверен с официальным справочником целей Метрики (счётчик 178943/63025594):

| goal_id | Название в Метрике | Шаг воронки в витрине | Исторические данные |
|---------|--------------------|-----------------------|---------------------|
| 3000178943 | Ecommerce: покупка | `order_paid` | ✅ 1 136 orders |
| 31297300 | Заказ оформлен (/checkout/success) | `order_created` | ✅ 1 424 |
| 21115915 | Оформление заказа (/simplecheckout) | `checkout_started` | ✅ 2 171 |
| 194388760 | Ecommerce: добавление в корзину | `add_to_cart` | ✅ 6 090 |
| 21115645 | Корзина (URL /cart) | `cart_visits` | ✅ 5 153 |
| 543662405 | Просмотр товара (view_product_details) | `product_views` | ⚠️ 0 (новая цель, ≥ апрель 2026) |
| 201398152 | Уникальный звонок (Calltouch) | `unique_calls` | ✅ 952 |

**Примечание по 543662xxx серии**: новые DataLayer/GTM-цели (view_product_details, add_to_cart event, checkout_open и др.) добавлены в Метрику после 28.03.2026. Колонки в `direct_custom_report` уже существуют, данные появятся при следующих выгрузках. Полный список — в `goals_sanok.md`.

**Нет спам-цели**: у sanok нет аналога magnetto-целей «Мусорный трафик» / «СПАМ» / «CRM: Спам заказ». `spam_traffic` колонка не нужна.

## Метрики, которые агент считает сам (в запросе, не хранятся)

| Метрика | Формула |
|---------|---------|
| CTR | `clicks / nullIf(impressions, 0) * 100` |
| CPC | `cost / nullIf(clicks, 0)` |
| Conv: клик→корзина | `add_to_cart / nullIf(clicks, 0) * 100` |
| Conv: корзина→чекаут | `checkout_started / nullIf(add_to_cart, 0) * 100` |
| Conv: чекаут→заказ | `order_created / nullIf(checkout_started, 0) * 100` |
| Conv: заказ→оплата | `order_paid / nullIf(order_created, 0) * 100` |
| CPO | `cost / nullIf(order_paid, 0)` |
| ROMI | `purchase_revenue / nullIf(cost, 0)` |
| ДРР (CRR) | `cost / nullIf(purchase_revenue, 0) * 100` |
| AOV | `purchase_revenue / nullIf(order_paid, 0)` |

## Сценарии использования

### 1. Топ кампаний по расходу за последние 30 дней

```sql
SELECT
    campaign_name,
    sum(clicks) AS clicks,
    round(sum(cost)) AS cost,
    sum(order_paid) AS orders,
    round(sum(purchase_revenue)) AS revenue,
    round(sum(purchase_revenue)/nullIf(sum(cost),0),1) AS romi,
    round(sum(cost)/nullIf(sum(order_paid),0)) AS cpo
FROM ym_sanok.dm_direct_performance FINAL
WHERE date >= today() - 30
GROUP BY campaign_name
ORDER BY cost DESC
LIMIT 20;
```

### 2. Поиск vs РСЯ за период

```sql
SELECT
    ad_network_type,
    sum(clicks)        AS clicks,
    round(sum(cost))   AS cost,
    sum(order_paid)    AS orders,
    round(sum(purchase_revenue)) AS revenue
FROM ym_sanok.dm_direct_performance FINAL
WHERE date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY ad_network_type;
```

### 3. Ecommerce-воронка по дням

```sql
SELECT
    date,
    sum(clicks)           AS clicks,
    sum(cart_visits)      AS cart_visits,
    sum(product_views)    AS product_views,   -- заполняется с апреля 2026
    sum(add_to_cart)      AS add_to_cart,
    sum(checkout_started) AS checkout_started,
    sum(order_created)    AS order_created,
    sum(order_paid)       AS order_paid,
    sum(unique_calls)     AS calls
FROM ym_sanok.dm_direct_performance FINAL
WHERE date >= today() - 14
GROUP BY date
ORDER BY date;
```

### 4. Лидирующие группы с нулевой окупаемостью

```sql
SELECT
    adgroup_id, adgroup_name, campaign_name,
    sum(clicks) AS clicks,
    round(sum(cost)) AS cost,
    sum(order_paid) AS orders
FROM ym_sanok.dm_direct_performance FINAL
WHERE date >= today() - 30
GROUP BY adgroup_id, adgroup_name, campaign_name
HAVING cost > 10000 AND orders = 0
ORDER BY cost DESC;
```

### 5. JOIN со справочником настроек (стратегия + CRR + статистика)

```sql
SELECT
    s.campaign_name,
    s.strategy_search_type,
    s.strategy_search_crr,
    round(sum(p.cost))   AS cost,
    sum(p.order_paid)    AS orders,
    round(sum(p.purchase_revenue)) AS revenue,
    round(sum(p.cost)/nullIf(sum(p.purchase_revenue),0)*100,1) AS drr
FROM (SELECT * FROM ym_sanok.dm_direct_performance FINAL) p
JOIN (SELECT * FROM ym_sanok.campaigns_settings FINAL) s
    ON toInt64(p.campaign_id) = s.campaign_id
WHERE p.date >= today() - 30
GROUP BY s.campaign_name, s.strategy_search_type, s.strategy_search_crr
ORDER BY cost DESC;
```

> **Важно**: `dm_direct_performance.campaign_id` — UInt64, `campaigns_settings.campaign_id` — Int64.
> JOIN через `USING (campaign_id)` упадёт с ошибкой типов. Всегда использовать
> `ON toInt64(p.campaign_id) = s.campaign_id`.

### 7. CPA по цели оптимизации кампании

**Триггеры**: "какая цель оптимизации даёт лучший CPA", "сравни кампании по цели стратегии",
"оптимизация на корзину работает лучше чем на покупку?", "какая автостратегия эффективнее"

Цель оптимизации хранится в `campaigns_settings.strategy_search_goal_id` (для поиска) и
`strategy_network_goal_id` (для РСЯ). Расшифровка — через `goal_dict`.

```sql
SELECT
    coalesce(g.goal_name, toString(cs.strategy_search_goal_id)) AS optimization_goal,
    cs.strategy_search_type                                      AS strategy_type,
    count(DISTINCT p.campaign_id)                                AS campaigns,
    round(sum(p.cost), 0)                                        AS total_cost,
    sum(p.order_paid)                                            AS purchases,
    sum(p.order_created)                                         AS orders_created,
    sum(p.checkout_started)                                      AS checkouts,
    round(sum(p.cost) / nullIf(sum(p.order_paid), 0), 0)        AS cpa_purchase,
    round(sum(p.cost) / nullIf(sum(p.order_created), 0), 0)     AS cpa_order,
    round(sum(p.cost) / nullIf(sum(p.checkout_started), 0), 0)  AS cpa_checkout
FROM (SELECT * FROM ym_sanok.dm_direct_performance FINAL) AS p
JOIN (SELECT * FROM ym_sanok.campaigns_settings FINAL) AS cs
    ON toInt64(p.campaign_id) = cs.campaign_id
LEFT JOIN ym_sanok.goal_dict g
    ON toUInt64(cs.strategy_search_goal_id) = g.goal_id
WHERE p.date >= today() - 90
  AND cs.strategy_search_goal_id IS NOT NULL
GROUP BY optimization_goal, strategy_type
ORDER BY total_cost DESC;
```

**Интерпретация**: сравни `cpa_purchase` между строками с разными `optimization_goal`.
Если кампании с `optimization_goal = 'Корзина'` дают `cpa_purchase` ниже,
чем с `optimization_goal = 'Оформление заказа'` — стратегия на более раннюю цель воронки
эффективнее для финальных покупок.

### 6. Конверсия воронки по кампаниям

```sql
SELECT
    campaign_name,
    sum(cart_visits)      AS cart_page_visits,
    sum(add_to_cart)      AS add_to_cart,
    sum(checkout_started) AS checkouts,
    sum(order_paid)       AS paid,
    round(sum(add_to_cart)      / nullIf(sum(cart_visits), 0) * 100, 1) AS cart2atc_pct,
    round(sum(checkout_started) / nullIf(sum(add_to_cart), 0) * 100, 1) AS atc2checkout_pct,
    round(sum(order_paid)       / nullIf(sum(checkout_started), 0) * 100, 1) AS checkout2paid_pct
FROM ym_sanok.dm_direct_performance FINAL
WHERE date >= today() - 30
GROUP BY campaign_name
HAVING add_to_cart > 10
ORDER BY paid DESC;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Сколько потратили» / «расход» | `sum(cost)` |
| «Сколько заработали» / «выручка» | `sum(purchase_revenue)` |
| «Оплаченные заказы» | `sum(order_paid)` |
| «Созданные заказы» | `sum(order_created)` |
| «Добавления в корзину» | `sum(add_to_cart)` |
| «Посещения корзины» | `sum(cart_visits)` |
| «Просмотры товара» | `sum(product_views)` (ненулевой с апреля 2026) |
| «Начали оформление» | `sum(checkout_started)` |
| «Звонки» | `sum(unique_calls)` |
| «Поиск или РСЯ лучше» | `GROUP BY ad_network_type` |
| «По дням / неделям / месяцам» | `GROUP BY date` |
| «По кампании / группе» | `GROUP BY campaign_id` / `adgroup_id` |
| «Окупаемость / ROMI / ДРР» | `purchase_revenue / cost` |
| «Настройки кампании» | JOIN `campaigns_settings` через `ON toInt64(campaign_id) = s.campaign_id` (не USING — разные типы) |
| «Настройки группы» | JOIN `adgroups_settings FINAL ON adgroup_id = group_id` |
| «Позиция / спецразмещение vs гарантия» | `impressions_premium`, `impressions_other`, `avg_impression_position` |
| «CTR по позиции» | `clicks_premium / nullIf(impressions_premium, 0)` vs `clicks_other / nullIf(impressions_other, 0)` |
| «Цель оптимизации vs CPA» / «какая автостратегия лучше» | Сценарий 7: JOIN `campaigns_settings` + `goal_dict` на `strategy_search_goal_id` |
