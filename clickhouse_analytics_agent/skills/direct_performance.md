# Скилл: Эффективность Яндекс Директа (ROAS, CPS, расходы)

Активируется при вопросах про: ROAS, окупаемость рекламы, расходы, бюджет, стоимость заказа,
CPS, CPC, эффективность кампаний Директа, сравнение кампаний по ROAS, атрибутированная выручка,
как окупается реклама, куда идёт бюджет, прибыльные/убыточные кампании.

---

## Модель атрибуции в витринах Direct

Используется **двухуровневая модель**:

| Поле | Смысл |
|------|-------|
| `orders_lastclick` | Заказы, оформленные **в том же визите**, что пришёл из Директа |
| `revenue_lastclick` | Выручка этих заказов (last-click, консервативная оценка) |
| `later_converters` | Уникальных клиентов, купивших **позже** — после визита из Директа |
| `orders_later_adj` | Взвешенное число таких заказов (доля, пропорциональная участию в цепочке) |
| `later_revenue_adj` | **30% выручки** от поздних покупок — доля, отдаваемая Директу как ассистенту |
| `total_orders` | `orders_lastclick` + `orders_later_adj` — итого заказов с учётом ассистирования |
| `total_attr_revenue` | `revenue_lastclick` + `later_revenue_adj` — итоговая атрибутированная выручка |
| `roas_lastclick` | `revenue_lastclick / cost` — консервативный ROAS |
| `roas_attributed` | `total_attr_revenue / cost` — ROAS с ассистированными конверсиями |
| `cps` | `cost / total_orders` — стоимость заказа (NULL если 0 заказов) |

**Правило интерпретации:**
- `roas_attributed` — основная метрика эффективности, использовать по умолчанию
- `roas_lastclick` — консервативная оценка, для кампаний с коротким циклом сделки
- Если `roas_attributed IS NULL` или `roas_lastclick IS NULL` — нет заказов за период, не делить вручную
- `cps IS NULL` — аналогично: 0 заказов за период

---

## Какую витрину использовать

| Задача | Витрина |
|--------|---------|
| ROAS, CPS, расходы, заказы по кампаниям | `dm_direct_enriched` |
| ROAS, эффективность по группам объявлений | `dm_direct_by_adgroup` |
| ROAS по регионам/городам | `dm_direct_by_geo` |
| Детали по показам, кликам, CTR, AvgCPC без выручки | `direct_campaigns` |

⚠️ **МК-кампании** (CampaignType = 'SMART_CAMPAIGN', или название начинается с `mk_` / `МК`):
у мастер-кампаний нет групп объявлений → `dm_direct_by_adgroup` для них пустая.
Анализ МК — только через `dm_direct_enriched`.

---

## Поля dm_direct_enriched

| Поле | Тип | Описание |
|------|-----|----------|
| `Date` | Date | Дата |
| `CampaignId` | UInt64 | ID кампании |
| `campaign_name` | String | Название кампании |
| `campaign_type` | String | Тип кампании (TEXT_CAMPAIGN и др.) |
| `impressions` | UInt64 | Показы |
| `clicks` | UInt64 | Клики |
| `cost` | Float64 | Расходы (руб.) |
| `sessions` | UInt64 | Сессии (из Метрики) |
| `conversions` | UInt64 | Конверсии — цели Директа |
| `avg_cpc` | Float64 | Средняя цена клика |
| `bounce_rate` | Float64 | Показатель отказов (%) |
| `avg_pageviews` | Float64 | Среднее число страниц |
| `conversion_rate` | Float64 | CR (%) |
| `orders_lastclick` | UInt64 | Заказы last-click |
| `revenue_lastclick` | Float64 | Выручка last-click (руб.) |
| `orders_later_adj` | Float64 | Заказы с ассистированием |
| `later_revenue_adj` | Float64 | Выручка ассистированных (руб.) |
| `later_converters` | UInt64 | Уникальных клиентов-ассистентов |
| `total_orders` | Float64 | Итого заказов (lastclick + assisted) |
| `total_attr_revenue` | Float64 | Итого атрибутированная выручка |
| `roas_lastclick` | Nullable(Float64) | ROAS консервативный |
| `roas_attributed` | Nullable(Float64) | ROAS с ассистированием |
| `cps` | Nullable(Float64) | Стоимость заказа |

---

## SQL-паттерны

### Топ кампаний по ROAS за период
```sql
SELECT
    campaign_name,
    sum(cost)                                        AS total_cost,
    sum(clicks)                                      AS total_clicks,
    sum(total_orders)                                AS total_orders,
    round(sum(total_attr_revenue))                   AS attr_revenue,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2) AS roas,
    round(sum(cost) / nullIf(sum(total_orders), 0)) AS cps
FROM dm_direct_enriched
WHERE Date >= today() - 30
GROUP BY campaign_name
HAVING total_cost > 0
ORDER BY roas DESC
LIMIT 20
```

### Сравнение двух моделей ROAS
```sql
SELECT
    campaign_name,
    round(sum(cost))                                              AS cost,
    round(sum(revenue_lastclick))                                 AS rev_lastclick,
    round(sum(total_attr_revenue))                                AS rev_attributed,
    round(sum(revenue_lastclick) / nullIf(sum(cost), 0), 2)      AS roas_lc,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas_attr,
    round(sum(cost) / nullIf(sum(total_orders), 0))              AS cps
FROM dm_direct_enriched
WHERE Date >= today() - 30
GROUP BY campaign_name
HAVING cost > 0
ORDER BY roas_attr DESC
```

### Динамика расходов и ROAS по дням
```sql
SELECT
    Date,
    sum(cost)                                                     AS daily_cost,
    sum(total_attr_revenue)                                       AS daily_revenue,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas,
    sum(total_orders)                                             AS orders
FROM dm_direct_enriched
WHERE Date >= today() - 60
GROUP BY Date
ORDER BY Date
```

### Сравнение текущего периода с предыдущим (WoW или MoM)
```sql
SELECT
    campaign_name,
    sumIf(cost, Date >= today() - 7)                             AS cost_7d,
    sumIf(cost, Date >= today() - 14 AND Date < today() - 7)     AS cost_prev_7d,
    round(sumIf(total_attr_revenue, Date >= today() - 7) /
          nullIf(sumIf(cost, Date >= today() - 7), 0), 2)        AS roas_7d,
    round(sumIf(total_attr_revenue, Date >= today() - 14 AND Date < today() - 7) /
          nullIf(sumIf(cost, Date >= today() - 14 AND Date < today() - 7), 0), 2) AS roas_prev_7d
FROM dm_direct_enriched
GROUP BY campaign_name
HAVING cost_7d > 0 OR cost_prev_7d > 0
ORDER BY cost_7d DESC
```

### Распределение бюджета по кампаниям
```sql
SELECT
    campaign_name,
    sum(cost)                                                     AS total_cost,
    round(sum(cost) / sum(sum(cost)) OVER () * 100, 1)           AS cost_share_pct,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas
FROM dm_direct_enriched
WHERE Date >= today() - 30
GROUP BY campaign_name
ORDER BY total_cost DESC
```

### Поиск убыточных кампаний (ROAS < 1)
```sql
SELECT
    campaign_name,
    sum(cost)                                                     AS cost,
    sum(total_attr_revenue)                                       AS revenue,
    round(sum(total_attr_revenue) / nullIf(sum(cost), 0), 2)     AS roas,
    sum(total_orders)                                             AS orders
FROM dm_direct_enriched
WHERE Date >= today() - 30
GROUP BY campaign_name
HAVING cost > 0
    AND (sum(total_attr_revenue) / nullIf(sum(cost), 0)) < 1
ORDER BY cost DESC
```

---

## Поля dm_direct_by_adgroup (дополнительные)

| Поле | Описание |
|------|----------|
| `AdGroupId` | ID группы |
| `adgroup_name` | Название группы |

Остальные поля идентичны dm_direct_enriched. Использовать для детализации по группам — **только для не-МК кампаний**.

---

## Правила интерпретации

- **nullIf(sum(cost), 0)** — всегда защищать деление от нулевого знаменателя
- **HAVING cost > 0** — фильтровать строки без расходов перед расчётом ROAS
- **Малые бюджеты (< 5000 ₽ за период)** → ставить ⚠️, ROAS статистически ненадёжен
- **later_revenue_adj** — 30% это фиксированная модельная доля, не настраиваемый параметр
- **Конверсии в Директе** (`conversions`) ≠ заказы (`orders_lastclick`) — конверсии это цели Метрики (включая добавление в корзину, звонки), заказы — только оформленные покупки
- **Всегда указывать период** в ответе: "за последние 30 дней (с ... по ...)"
- **Не суммировать roas_attributed** напрямую — он не аддитивен, считать через sum(revenue)/sum(cost)
