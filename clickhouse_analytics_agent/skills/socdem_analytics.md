# Скилл: Социально-демографический анализ рекламы

Активируется при вопросах про: возраст аудитории, пол, доход, устройства,
социодем, кто кликает, кто конвертирует, автотаргетинг vs ключи,
мобайл vs десктоп, TargetingCategory, CriterionType, ROAS по возрасту.

---

## Таблица socdem_direct_analytics

Социально-демографический срез рекламы Яндекс Директа. Каждая строка:
**дата x кампания x группа x гео x тип сети x тип таргетинга x возраст x пол x доход x устройство**.

**Движок:** MergeTree (без FINAL). 148 356 строк, 28 кампаний, период 2025-09-01 — 2026-03-28.
**Обновление:** REFRESHABLE MV, ежедневно в 02:00 UTC.

---

## Поля

| Поле | Описание |
|------|----------|
| `Date` | Дата |
| `CampaignId` | ID кампании |
| `CampaignName` | Название кампании |
| `AdGroupName` | Название группы |
| `LocationOfPresenceName` | Город/регион (фактическое нахождение пользователя) |
| `AdNetworkType` | `SEARCH` / `AD_NETWORK` |
| `CriterionType` | Тип таргетинга (см. ниже) |
| `TargetingCategory` | Категория соответствия запросу (см. ниже) |
| `Age` | Возрастная группа |
| `Gender` | `GENDER_FEMALE` / `GENDER_MALE` / `UNKNOWN` |
| `IncomeGrade` | Уровень дохода |
| `Device` | `MOBILE` / `DESKTOP` / `TABLET` / `SMART_TV` |
| `Impressions` / `Clicks` / `Cost` | Трафик и расход (руб) |
| `Sessions` / `Bounces` | Сессии и отказы |
| `MacroConversions` | Сумма 9 ключевых целей (покупка, заказ, корзина, звонок и др.) |
| `MicroConversions` | Сумма всех 57 целей |
| `Revenue` | Выручка (PurchaseRevenue из Директа, LSCCD) |

---

## Справочники значений

### CriterionType — тип таргетинга
| Значение | Описание | Строк |
|----------|----------|-------|
| `AUTOTARGETING` | Автотаргетинг | 105 742 |
| `OFFER_RETARGETING` | Смарт-баннеры / ретаргетинг по фиду | 25 787 |
| `KEYWORD` | Ключевое слово | 9 543 |
| `FEED_FILTER` | Фид с фильтрами товаров | 6 455 |
| `RETARGETING` | Ретаргетинг по аудитории | 829 |

### TargetingCategory — соответствие запросу (только SEARCH)
| Значение | Описание |
|----------|----------|
| `EXACT` | Точное совпадение |
| `ALTERNATIVE` | Альтернативные запросы |
| `NARROW` | Уточнённые запросы |
| `BROADER` | Более широкие запросы |
| `ACCESSORY` | Сопутствующие |
| `UNKNOWN` | РСЯ или данные недоступны |

### Age — возрастная группа
`AGE_0_17`, `AGE_18_24`, `AGE_25_34`, `AGE_35_44`, `AGE_45_54`, `AGE_55`, `UNKNOWN`

### IncomeGrade — уровень дохода
`VERY_HIGH`, `HIGH`, `ABOVE_AVERAGE`, `OTHER` (средний и ниже / не определён)

### MacroConversions — состав (9 целей)
Ecommerce: покупка (3000178943), Заказ оформлен (31297300), Оформление заказа (21115915), Начало оформления (495725161), Добавление в корзину (194388760), Корзина (21115645), Звонок Calltouch (34740969), Контакты (21115920), Более 3-х страниц (157742788).

> `MacroConversions` НЕ РАВНО заказам — включает корзину, звонки и вовлечённость. Для чистых заказов используй `dm_direct_performance.order_paid`.

---

## SQL-паттерны

### По возрасту
```sql
SELECT Age,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS macro_conv,
       round(sum(Cost) / nullIf(sum(MacroConversions), 0)) AS cpa,
       round(sum(Revenue)) AS revenue,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30
GROUP BY Age ORDER BY revenue DESC
```

### По устройству x сети
```sql
SELECT Device, AdNetworkType,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Cost) / nullIf(sum(MacroConversions), 0)) AS cpa,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas,
       round(sum(Bounces) / nullIf(sum(Sessions), 0) * 100, 1) AS bounce_pct
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30
GROUP BY Device, AdNetworkType ORDER BY cost DESC
```

### По полу
```sql
SELECT Gender,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Revenue)) AS revenue,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30
GROUP BY Gender ORDER BY revenue DESC
```

### По уровню дохода
```sql
SELECT IncomeGrade,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Revenue)) AS revenue,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30
GROUP BY IncomeGrade ORDER BY revenue DESC
```

### Тип таргетинга: автотаргетинг vs ключи vs смарт-баннеры
```sql
SELECT CriterionType, AdNetworkType,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Cost) / nullIf(sum(MacroConversions), 0)) AS cpa,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30
GROUP BY CriterionType, AdNetworkType ORDER BY cost DESC
```

### Точность таргетинга (TargetingCategory) — только поиск
```sql
SELECT TargetingCategory,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Cost) / nullIf(sum(MacroConversions), 0)) AS cpa,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30 AND AdNetworkType = 'SEARCH'
GROUP BY TargetingCategory ORDER BY cost DESC
```

### Матрица возраст x устройство
```sql
SELECT Age, Device,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30
GROUP BY Age, Device
ORDER BY
    CASE Age WHEN 'AGE_18_24' THEN 1 WHEN 'AGE_25_34' THEN 2 WHEN 'AGE_35_44' THEN 3
             WHEN 'AGE_45_54' THEN 4 WHEN 'AGE_55' THEN 5 ELSE 6 END,
    CASE Device WHEN 'DESKTOP' THEN 1 WHEN 'MOBILE' THEN 2 ELSE 3 END
```

### ТОП городов
```sql
SELECT LocationOfPresenceName AS city,
       sum(Clicks) AS clicks, round(sum(Cost)) AS cost,
       sum(MacroConversions) AS conversions,
       round(sum(Revenue)) AS revenue,
       round(sum(Revenue) / nullIf(sum(Cost), 0), 1) AS roas
FROM ym_sanok.socdem_direct_analytics
WHERE Date >= today() - 30 AND LocationOfPresenceName != ''
GROUP BY city ORDER BY revenue DESC LIMIT 20
```

---

## Правила интерпретации

- **NO FINAL** — MergeTree, дедупликация не нужна
- **MacroConversions != заказы** — включает корзину, звонки, вовлечённость
- **Revenue** — атрибуция LSCCD, может расходиться с `dm_orders`
- **LocationOfPresenceName** — место нахождения пользователя, не таргетинг. Пустая строка = не определён
- **UNKNOWN** в Age/Gender/IncomeGrade — Яндекс не определил сегмент
- **Связь**: `CampaignId` → `campaigns_settings` для стратегии; `dm_direct_performance` для детальной ecommerce-воронки без социодем
