# socdem_direct_analytics — Sanok

## Назначение

Социально-демографический срез рекламы Яндекс Директа. Каждая строка — это комбинация
**дата × кампания × группа × гео × тип сети × тип таргетинга × возраст × пол × доход × устройство**.
Позволяет отвечать на вопросы: «кто кликает», «кто конвертирует», «насколько эффективен таргетинг
по сегментам».

## Источник и обновление

- **Читается из**: `ym_sanok.direct_custom_report` (лог Яндекс Директа)
- **MV**: `mv_socdem_direct_analytics` — REFRESHABLE MV, `REFRESH EVERY 1 DAY OFFSET 2 HOUR`
- **Обновление**: ежедневно в 02:00 UTC (полная пересборка)
- **Покрытие**: 2025-09-01 — 2026-03-28 (148 356 строк, 28 кампаний)

## Структура таблицы

```sql
CREATE TABLE ym_sanok.socdem_direct_analytics (
    Date                   Date,
    CampaignId             UInt64,
    CampaignName           String,
    AdGroupName            String,
    LocationOfPresenceName String,      -- название города/региона
    AdNetworkType          String,      -- SEARCH / AD_NETWORK
    CriterionType          String,      -- тип таргетинга (см. ниже)
    TargetingCategory      String,      -- соответствие запросу (см. ниже)
    Age                    String,      -- возрастная группа (см. ниже)
    Gender                 String,      -- GENDER_FEMALE / GENDER_MALE / UNKNOWN
    IncomeGrade            String,      -- уровень дохода (см. ниже)
    Device                 String,      -- MOBILE / DESKTOP / TABLET / SMART_TV
    Impressions            UInt64,
    Clicks                 UInt64,
    Cost                   Float64,     -- расход в рублях
    Sessions               UInt64,      -- сессии Метрики
    Bounces                UInt64,
    MacroConversions       UInt64,      -- сумма ключевых целей (см. ниже)
    MicroConversions       UInt64,      -- сумма всех 57 целей
    Revenue                Float64      -- выручка (PurchaseRevenue из Директа, LSCCD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Date)
ORDER BY (Date, CampaignId, AdGroupName, Age, Gender, Device)
```

## Справочники значений

### CriterionType — тип таргетинга
| Значение | Описание | Строк |
|----------|----------|-------|
| `AUTOTARGETING` | Автотаргетинг (Директ сам подбирает аудиторию) | 105 742 |
| `OFFER_RETARGETING` | Смарт-баннеры / ретаргетинг по фиду | 25 787 |
| `KEYWORD` | Ключевое слово | 9 543 |
| `FEED_FILTER` | Фид с фильтрами товаров | 6 455 |
| `RETARGETING` | Ретаргетинг по аудитории | 829 |

### TargetingCategory — категория соответствия (только SEARCH)
| Значение | Описание |
|----------|----------|
| `EXACT` | Точное совпадение с ключевой фразой |
| `ALTERNATIVE` | Альтернативные запросы |
| `NARROW` | Узкие (уточнённые) запросы |
| `BROADER` | Более широкие запросы |
| `ACCESSORY` | Сопутствующие запросы |
| `UNKNOWN` | РСЯ или данные недоступны |

### Age — возрастная группа
| Значение | Диапазон |
|----------|---------|
| `AGE_18_24` | 18–24 года |
| `AGE_25_34` | 25–34 года |
| `AGE_35_44` | 35–44 года |
| `AGE_45_54` | 45–54 года |
| `AGE_55` | 55+ лет |
| `AGE_0_17` | до 17 лет |
| `UNKNOWN` | возраст не определён |

### IncomeGrade — уровень дохода (по Яндексу)
| Значение | Описание |
|----------|---------|
| `VERY_HIGH` | Очень высокий доход |
| `HIGH` | Высокий доход |
| `ABOVE_AVERAGE` | Выше среднего |
| `OTHER` | Средний и ниже / не определён |

### MacroConversions — состав (goal_id)
Сумма конверсий по 9 ключевым целям:

| goal_id | Название |
|---------|----------|
| 3000178943 | Ecommerce: покупка |
| 31297300 | Заказ оформлен (/checkout/success) |
| 21115915 | Оформление заказа (/simplecheckout) |
| 495725161 | Автоцель: начало оформления заказа |
| 194388760 | Ecommerce: добавление в корзину |
| 21115645 | Корзина (/cart) |
| 34740969 | Звонок Calltouch |
| 21115920 | Контакты |
| 157742788 | Более 3-х страниц |

> `MicroConversions` — сумма всех 57 целей Метрики (включая автоцели, jivo, категорийные страницы).
> Используй `MacroConversions` для оценки бизнес-результата, `MicroConversions` — для
> анализа активности/вовлечённости.

## Реальные данные (2026-01-01 — 2026-03-28)

| Метрика | Значение |
|---------|---------|
| Показы | 1 241 441 |
| Клики | 83 693 |
| Расход | 2 069 514 руб |
| Выручка | 201 677 620 руб |
| MacroConversions | 26 416 |
| MicroConversions | 135 337 |
| CTR | ~6.7% |
| ROAS | ~97.5x |

## Сценарии использования для AI-агента

### 1. Эффективность по возрасту

**Триггеры**: "Какой возраст лучше конвертирует?", "CTR по возрасту", "Откуда идут покупки по возрасту?"

```sql
SELECT
    Age,
    sum(Impressions)                                        AS impressions,
    sum(Clicks)                                             AS clicks,
    round(sum(Clicks) / nullIf(sum(Impressions), 0) * 100, 2) AS ctr_pct,
    round(sum(Cost), 0)                                     AS cost,
    round(sum(Cost) / nullIf(sum(Clicks), 0), 1)            AS cpc,
    sum(MacroConversions)                                   AS macro_conv,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)  AS cpa,
    round(sum(Revenue), 0)                                  AS revenue,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)           AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
GROUP BY Age
ORDER BY revenue DESC
```

### 2. Эффективность по устройству

**Триггеры**: "Мобильные vs десктоп", "С какого устройства больше покупок?", "CPA по устройствам"

```sql
SELECT
    Device,
    AdNetworkType,
    sum(Impressions)                                           AS impressions,
    sum(Clicks)                                                AS clicks,
    round(sum(Cost), 0)                                        AS cost,
    sum(MacroConversions)                                      AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)     AS cpa,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)              AS roas,
    round(sum(Bounces) / nullIf(sum(Sessions), 0) * 100, 1)    AS bounce_pct
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
GROUP BY Device, AdNetworkType
ORDER BY cost DESC
```

### 3. Эффективность по полу

**Триггеры**: "Мужчины или женщины лучше конвертируют?", "CTR по полу", "Выручка по гендеру"

```sql
SELECT
    Gender,
    sum(Impressions)                                          AS impressions,
    sum(Clicks)                                               AS clicks,
    round(sum(Cost), 0)                                       AS cost,
    sum(MacroConversions)                                     AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)    AS cpa,
    round(sum(Revenue), 0)                                    AS revenue,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)             AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
GROUP BY Gender
ORDER BY revenue DESC
```

### 4. Эффективность по уровню дохода

**Триггеры**: "Аудитория с высоким доходом конвертирует лучше?", "ROAS по income grade", "CPA по уровню дохода"

```sql
SELECT
    IncomeGrade,
    sum(Clicks)                                               AS clicks,
    round(sum(Cost), 0)                                       AS cost,
    sum(MacroConversions)                                     AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)    AS cpa,
    round(sum(Revenue), 0)                                    AS revenue,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)             AS roas,
    round(sum(Revenue) / nullIf(sum(Clicks), 0), 0)           AS revenue_per_click
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
GROUP BY IncomeGrade
ORDER BY revenue DESC
```

### 5. Тип таргетинга: что работает лучше

**Триггеры**: "Автотаргетинг vs ключевые слова", "Смарт-баннеры vs ретаргетинг", "CriterionType эффективность"

```sql
SELECT
    CriterionType,
    AdNetworkType,
    sum(Clicks)                                               AS clicks,
    round(sum(Cost), 0)                                       AS cost,
    sum(MacroConversions)                                     AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)    AS cpa,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)             AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
GROUP BY CriterionType, AdNetworkType
ORDER BY cost DESC
```

### 6. ТОП городов по эффективности

**Триггеры**: "Из каких городов больше покупок?", "Лучшие регионы", "CPA по городам"

```sql
SELECT
    LocationOfPresenceName                                    AS city,
    sum(Clicks)                                               AS clicks,
    round(sum(Cost), 0)                                       AS cost,
    sum(MacroConversions)                                     AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)    AS cpa,
    round(sum(Revenue), 0)                                    AS revenue,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)             AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
  AND LocationOfPresenceName != ''
GROUP BY city
ORDER BY revenue DESC
LIMIT 20
```

### 7. Матрица возраст × устройство

**Триггеры**: "Кто активнее — молодёжь на мобильных или зрелые на десктопе?", "Сегментация аудитории"

```sql
SELECT
    Age,
    Device,
    sum(Clicks)                                               AS clicks,
    round(sum(Cost), 0)                                       AS cost,
    sum(MacroConversions)                                     AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)    AS cpa,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)             AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
GROUP BY Age, Device
ORDER BY
    CASE Age
        WHEN 'AGE_18_24' THEN 1 WHEN 'AGE_25_34' THEN 2 WHEN 'AGE_35_44' THEN 3
        WHEN 'AGE_45_54' THEN 4 WHEN 'AGE_55' THEN 5 ELSE 6
    END,
    CASE Device WHEN 'DESKTOP' THEN 1 WHEN 'MOBILE' THEN 2 WHEN 'TABLET' THEN 3 ELSE 4 END
```

### 8. Соответствие запросу (TargetingCategory) — поиск

**Триггеры**: "Насколько точно Директ попадает в аудиторию?", "Альтернативные vs точные запросы", "TargetingCategory vs CPA"

```sql
SELECT
    TargetingCategory,
    sum(Clicks)                                               AS clicks,
    round(sum(Cost), 0)                                       AS cost,
    sum(MacroConversions)                                     AS conversions,
    round(sum(Cost) / nullIf(sum(MacroConversions), 0), 0)    AS cpa,
    round(sum(Revenue) / nullIf(sum(Cost), 0), 1)             AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= '2026-01-01'
  AND AdNetworkType = 'SEARCH'
GROUP BY TargetingCategory
ORDER BY cost DESC
```

## Технические уточнения

**NO FINAL**: таблица `MergeTree` (не `ReplacingMergeTree`) — запросы без `FINAL`, дедупликация не нужна.

**MacroConversions ≠ orders**: включает не только покупки, но и добавления в корзину, оформление, звонки. Для чистого подсчёта заказов используй `dm_direct_performance.order_created` или `dm_direct_performance.order_paid`.

**Revenue**: атрибуция Директа (LSCCD — Last Significant Click Cross Device). Может расходиться с фактическими заказами из `dm_orders`.

**Дата обновления**: REFRESHABLE MV пересобирает таблицу целиком каждую ночь в 02:00 UTC. Данные за текущий день появятся на следующее утро.

**LocationOfPresenceName**: населённый пункт, где **находится** пользователь (не таргетинг). Пустая строка = данные не определены.

**Связь с другими таблицами**:
- `campaigns_settings` → расшифровать стратегию кампании по `CampaignId`
- `dm_direct_performance` → детальная статистика по целям воронки (без социодем)
- `goal_dict` → расшифровать goal_id в `MacroConversions` / `MicroConversions`
