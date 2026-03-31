# Скилл: Анализ ключевых слов и поисковых запросов

Активируется при вопросах про: ключевые слова, ключи, запросы, поисковые запросы,
автотаргетинг, минус-слова, ставки, позиции объявлений, слот, CTR по позиции,
что ищут пользователи, keyword, критерии, тип соответствия, AvgEffectiveBid,
эффективность ключей, дорогие/дешёвые ключевые слова.

---

## Какую таблицу использовать

| Задача | Таблица |
|--------|---------|
| Расходы, CTR, конверсии по ключам/критериям | `direct_criteria` |
| Реальные поисковые запросы пользователей | `direct_search_queries` |
| Расходы по позиции (слоту) в агрегате | `direct_criteria` GROUP BY Slot |
| Топ слов, запускающих объявления | `direct_search_queries` GROUP BY Query |

---

## Таблица direct_criteria — критерии и ключи

### Поля
| Поле | Тип | Описание |
|------|-----|----------|
| `Date` | Date | Дата |
| `CampaignId` | UInt64 | ID кампании |
| `CampaignName` | String | Название кампании |
| `AdGroupId` | UInt64 | ID группы |
| `AdGroupName` | String | Название группы |
| `CriterionId` | UInt64 | ID критерия |
| `Criterion` | String | Текст критерия (ключевое слово или тип) |
| `CriterionType` | String | Тип критерия (см. ниже) |
| `AdNetworkType` | String | Сеть: SEARCH / NETWORK |
| `Device` | String | Устройство: DESKTOP / MOBILE / TABLET |
| `Slot` | String | Позиция объявления (см. ниже) |
| `MatchType` | String | Тип соответствия: KEYWORD / SYNONYM / RELATED_KEYWORD и др. |
| `LocationOfPresenceName` | String | Город/регион пользователя (на русском) |
| `Impressions` | UInt64 | Показы |
| `Clicks` | UInt64 | Клики |
| `Cost` | Float64 | Расходы (руб.) |
| `Ctr` | Float64 | CTR (%) |
| `AvgCpc` | Float64 | Средняя цена клика |
| `AvgEffectiveBid` | Float64 | Средняя ставка на момент показа |
| `Conversions` | UInt64 | Конверсии (цели Директа) |
| `ConversionRate` | Float64 | CR (%) |
| `CostPerConversion` | Float64 | Цена конверсии |
| `Revenue` | Float64 | Выручка из Директа (не из Метрики — использовать осторожно) |

### CriterionType — типы критериев
| Тип | Смысл | Доля бюджета |
|-----|-------|-------------|
| `AUTOTARGETING` | МК (мастер-кампании) — Директ сам подбирает запросы | ~78% |
| `OFFER_RETARGETING` | Смарт-баннеры — ретаргетинг по товарам | ~13% |
| `KEYWORD` | Классические ключевые слова | ~8% |
| `FEED_FILTER` | Фильтры по товарному фиду (товарные галереи) | ~1% |
| `RETARGETING` | Аудиторный ретаргетинг | <1% |

⚠️ **AUTOTARGETING** — для МК у него нет группы (`AdGroupName` может быть пустым или технический), ключевые слова не управляются вручную. Анализ ключей имеет смысл только для `CriterionType = 'KEYWORD'`.

### Slot — позиции показа
| Слот | Описание |
|------|----------|
| `PREMIUMBLOCK` | Топ поисковой выдачи (спецразмещение) |
| `PRODUCT_GALLERY` | Товарная галерея Яндекса |
| `OTHER` | РСЯ, ретаргетинг, остальные позиции |
| `ALONE` | Единственное объявление на странице |
| `SUGGEST` | Поисковые подсказки |
| `CPA_NETWORK` | CPA-сети |

---

## Таблица direct_search_queries — поисковые запросы

### Поля
| Поле | Тип | Описание |
|------|-----|----------|
| `Date` | Date | Дата |
| `CampaignId` | UInt64 | ID кампании |
| `AdGroupId` | UInt64 | ID группы |
| `CriterionId` | UInt64 | ID сработавшего критерия |
| `Criterion` | String | Критерий, который сработал на запрос |
| `Query` | String | Реальный поисковый запрос пользователя |
| `MatchType` | String | Как запрос совпал с критерием (см. ниже) |
| `Device` | String | Устройство |
| `Impressions` | UInt64 | Показы |
| `Clicks` | UInt64 | Клики |
| `Cost` | Float64 | Расходы (руб.) |
| `Ctr` | Float64 | CTR (%) |
| `AvgCpc` | Float64 | Средняя цена клика |
| `Conversions` | UInt64 | Конверсии |

### MatchType — тип соответствия запроса критерию
| Тип | Смысл |
|-----|-------|
| `NONE` | Автотаргетинг — Директ сам сопоставил (без явного ключа) |
| `SYNONYM` | Синоним или семантически близкий запрос |
| `KEYWORD` | Точное или близкое к точному совпадение с ключевым словом |

⚠️ `Revenue` в `direct_search_queries` **отсутствует** — только конверсии (цели Директа). Для выручки по запросам джойн невозможен. Использовать `Conversions` как прокси.

---

## SQL-паттерны

### Топ ключевых слов по расходам (только KEYWORD-тип)
```sql
SELECT
    CampaignName,
    AdGroupName,
    Criterion,
    sum(Clicks)                                               AS clicks,
    sum(Cost)                                                 AS cost,
    sum(Conversions)                                          AS conversions,
    round(sum(Cost) / nullIf(sum(Clicks), 0), 2)             AS avg_cpc,
    round(sum(Conversions) / nullIf(sum(Clicks), 0) * 100, 2) AS cr_pct
FROM direct_criteria
WHERE Date >= today() - 30
    AND CriterionType = 'KEYWORD'
GROUP BY CampaignName, AdGroupName, Criterion
HAVING cost > 0
ORDER BY cost DESC
LIMIT 30
```

### CTR и расходы по слотам (позициям)
```sql
SELECT
    Slot,
    sum(Impressions)                                          AS impressions,
    sum(Clicks)                                               AS clicks,
    sum(Cost)                                                 AS cost,
    round(sum(Clicks) / nullIf(sum(Impressions), 0) * 100, 2) AS ctr_pct,
    round(sum(Cost) / nullIf(sum(Clicks), 0), 2)             AS avg_cpc
FROM direct_criteria
WHERE Date >= today() - 30
    AND CriterionType = 'KEYWORD'
GROUP BY Slot
ORDER BY cost DESC
```

### Топ реальных поисковых запросов по кликам
```sql
SELECT
    Query,
    sum(Impressions)                                          AS impressions,
    sum(Clicks)                                               AS clicks,
    sum(Cost)                                                 AS cost,
    sum(Conversions)                                          AS conversions,
    round(sum(Clicks) / nullIf(sum(Impressions), 0) * 100, 2) AS ctr_pct,
    round(sum(Cost) / nullIf(sum(Clicks), 0), 2)             AS avg_cpc
FROM direct_search_queries
WHERE Date >= today() - 30
GROUP BY Query
ORDER BY clicks DESC
LIMIT 50
```

### Запросы с конверсиями (что реально конвертирует)
```sql
SELECT
    Query,
    sum(Clicks)                                               AS clicks,
    sum(Cost)                                                 AS cost,
    sum(Conversions)                                          AS conversions,
    round(sum(Cost) / nullIf(sum(Conversions), 0), 0)        AS cost_per_conv
FROM direct_search_queries
WHERE Date >= today() - 30
    AND Conversions > 0
GROUP BY Query
HAVING conversions >= 2
ORDER BY conversions DESC
LIMIT 30
```

### Неэффективные запросы (высокие расходы, 0 конверсий)
```sql
SELECT
    Query,
    sum(Clicks)       AS clicks,
    sum(Cost)         AS cost,
    sum(Conversions)  AS conversions
FROM direct_search_queries
WHERE Date >= today() - 30
GROUP BY Query
HAVING cost > 500 AND conversions = 0
ORDER BY cost DESC
LIMIT 30
```

### Ставки и конкурентность по ключам
```sql
SELECT
    Criterion,
    AdGroupName,
    round(avg(AvgEffectiveBid))  AS avg_bid,
    round(avg(AvgCpc))           AS avg_cpc,
    round(avg(Ctr), 2)           AS avg_ctr,
    sum(Clicks)                  AS clicks,
    sum(Cost)                    AS cost
FROM direct_criteria
WHERE Date >= today() - 30
    AND CriterionType = 'KEYWORD'
    AND AvgEffectiveBid > 0
GROUP BY Criterion, AdGroupName
ORDER BY avg_bid DESC
LIMIT 30
```

### Распределение запросов по типу соответствия
```sql
SELECT
    MatchType,
    count()           AS unique_queries,
    sum(Clicks)       AS clicks,
    sum(Cost)         AS cost,
    sum(Conversions)  AS conversions
FROM direct_search_queries
WHERE Date >= today() - 30
GROUP BY MatchType
ORDER BY cost DESC
```

---

## Правила интерпретации

- **AUTOTARGETING занимает ~78% бюджета** — это нормально для МК. Не пытаться "оптимизировать" ключи там.
- **Ключевые слова (KEYWORD)** — только ~8% бюджета — это классические поисковые кампании (Поиск | Бренд, Поиск | Ванны и т.д.)
- **Запросы с MatchType=NONE** — автотаргетинг, Директ сам решил показать. Много таких — нормально.
- **Criterion `---autotargeting`** — системное значение для автотаргетинга в МК, не ошибка
- **AvgEffectiveBid = 0** — показ был бесплатным или данные недоступны, фильтровать при анализе ставок
- **CostPerConversion в direct_criteria** — цена цели Директа (не заказа), может быть намного ниже реального CPS
- **Малые выборки (< 10 кликов по ключу)** → ставить ⚠️, CTR и CR ненадёжны
