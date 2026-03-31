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
