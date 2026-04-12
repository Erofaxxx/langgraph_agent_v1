# bad_placements_v3 — Площадки РСЯ

## Назначение

Ежедневный рейтинг площадок рекламной сети Яндекса (РСЯ), на которых показывалась реклама sanok. Для каждой площадки рассчитаны метрики за 60 дней, отклонение от эталона по кампании и автоматический вердикт. Позволяет находить сайты и приложения, которые дают нецелевой или дорогой трафик, и площадки-лидеры, заслуживающие масштабирования.

Типичные задачи: составить список площадок к исключению, найти площадки с аномально высоким CPC или bounce_rate, выявить нишевые площадки с хорошей конверсией.

## Источник данных и окно

- Таблица: `ym_sanok.direct_custom_report`
- Фильтр: `AdNetworkType = 'AD_NETWORK'` и `Placement != ''`
- Окно: **60 дней** скользящих от последней даты в отчёте
- Обновление: `REFRESH EVERY 1 DAY OFFSET 6 HOUR`

## Ключевые метрики

### goal_score — взвешенный балл конверсий

Те же веса, что и в `bad_keywords_v1` — единая шкала по аккаунту:

| Вес | Цели |
|-----|------|
| ×10 | Ecommerce: покупка · Целевой звонок · Уникальный звонок |
| ×8  | Jivo: чат начат клиентом · Заказ оформлен |
| ×5  | Оформление заказа · Заполнил контактные данные |
| ×2  | Добавление в корзину · Корзина |

`goal_score_rate = (goal_score / clicks) × 100` — конверсионность площадки на 100 кликов.

### Бенчмарки — эталон по кампании

Для каждой кампании считаются собственные эталоны **взвешенные по кликам** (не аккаунтовые):

- `avg_cpc_campaign` — средний CPC по всем площадкам кампании
- `bench_roas_campaign` — средний ROAS по кампании
- `bench_goal_score_rate` — средний goal_score_rate по кампании

### is_recent
`1` = площадка активна в последние 20 дней. `0` = площадка давно не показывалась — вердикт теряет актуальность.

### Отклонения от бенчмарка

- `cpc_deviation` — насколько CPC площадки выше/ниже среднего по кампании: `+0.5` = дороже на 50%, `-0.3` = дешевле на 30%
- `goal_rate_deviation` — отклонение goal_score_rate от бенчмарка: `-1.0` если конверсий нет
- `roas_deviation` — отклонение ROAS: `-1.0` если выручки нет

## Автоматический вердикт (zone_status + zone_reason)

Вердикт — стартовая точка, а не приговор. При нехватке данных алгоритм консервативно ставит `pending`.

### pending
Площадка неактивна (`is_recent = 0`) ИЛИ данных мало (`clicks < 10` или `cost < 200`).

### red — исключить или проверить
Любое из условий:
- Нет целевых действий и нет выручки при расходе > 400 руб
- CPC втрое выше среднего по кампании — и ни одной конверсии
- Нет выручки, расход > 250, goal_score_rate вдвое ниже бенчмарка

`zone_reason`: `r:no_goals+cost>400` · `r:cpc>3x+no_goals` · `r:no_roas+low_gsr+cost>250`

### green — перспективная площадка
Любое из условий:
- ROAS от 2 до 50 при CPC не выше 1.5× среднего
- ROAS > 50 — выдающийся результат
- goal_score_rate в 3–5× выше бенчмарка при нормальном CPC
- goal_score_rate в 5× и более выше бенчмарка — вне зависимости от CPC

`zone_reason`: `g:roas_2-50+cpc_ok` · `g:roas>50` · `g:gsr_3-5x+cpc_ok` · `g:gsr>5x`

### yellow — неоднозначно
Не провалилась до red, не показала явного результата. Требует контекстного суждения.

## Когда zone_status можно пересмотреть

- **Нишевые сантехнические сайты в red** — специализированные ресурсы (например, форумы сантехников или каталоги ванн) могут давать качественную аудиторию с долгим циклом принятия решений; конверсий в окне 60 дней нет, но трафик целевой
- **Площадка в pending из-за is_recent = 0** — показывалась давно и показала хороший результат; стоит проверить, не исключена ли случайно
- **Площадка в yellow с высоким bounce_rate** — алгоритм не учитывает bounce_rate при yellow; агент может самостоятельно принять решение о red
- **Маркетплейсы и агрегаторы** (ozon.ru, wildberries.ru) — могут давать высокий CPC из-за конкурентной аудитории, но при этом быть целевыми покупателями; смотреть на goal_score вручную

В этих случаях смотреть на `goal_score`, `bounce_rate`, `cost`, `cpc_deviation` и выносить суждение поверх zone_status.

## Структура таблицы

| Поле | Тип | Описание |
|------|-----|----------|
| report_date | Date | Дата расчёта |
| Placement | String | Домен сайта или ID приложения |
| CampaignId | UInt64 | ID кампании |
| CampaignName | String | Название кампании |
| cost | Float64 | Расход, руб за 60 дней |
| clicks | UInt64 | Клики |
| impressions | UInt64 | Показы |
| cpc | Nullable(Float64) | Фактический CPC, руб |
| purchase_revenue | Float64 | Выручка (атрибуция Директа) |
| roas | Nullable(Float64) | Выручка / расход |
| goal_score | Float64 | Взвешенный балл конверсий |
| goal_score_rate | Nullable(Float64) | goal_score на 100 кликов |
| bounces | UInt64 | Отказы |
| bounce_rate | Nullable(Float64) | Доля отказов, % |
| is_recent | UInt8 | 1 = активна в последние 20 дней |
| cpc_deviation | Nullable(Float64) | Отклонение CPC от среднего по кампании |
| goal_rate_deviation | Nullable(Float64) | Отклонение goal_score_rate от бенчмарка |
| roas_deviation | Nullable(Float64) | Отклонение ROAS от бенчмарка |
| avg_cpc_campaign | Nullable(Float64) | Средний CPC по кампании (эталон) |
| bench_roas_campaign | Nullable(Float64) | Средний ROAS по кампании (эталон) |
| bench_goal_score_rate | Nullable(Float64) | Средний goal_score_rate по кампании (эталон) |
| zone_status | String | green / yellow / red / pending |
| zone_reason | String | Машиночитаемая причина вердикта |

## Сценарии использования для AI-агента

### 1. Красные площадки — кандидаты к исключению

**Триггеры:** «Какие площадки плохие?», «Что исключить из РСЯ?», «Площадки без конверсий»

```sql
SELECT
    Placement, CampaignName,
    cost, clicks, cpc, avg_cpc_campaign,
    goal_score, bounce_rate,
    zone_reason
FROM ym_sanok.bad_placements_v3
WHERE zone_status = 'red'
ORDER BY cost DESC
LIMIT 30;
```

### 2. Зелёные площадки — масштабировать или добавить в whitelist

**Триггеры:** «Лучшие площадки РСЯ», «Где хорошо конвертируют?», «Площадки для масштабирования»

```sql
SELECT
    Placement, CampaignName,
    cost, clicks, roas, goal_score_rate,
    bench_goal_score_rate,
    round(goal_rate_deviation * 100, 0) AS gsr_deviation_pct,
    zone_reason
FROM ym_sanok.bad_placements_v3
WHERE zone_status = 'green'
ORDER BY goal_score_rate DESC;
```

### 3. Дорогие площадки без конверсий

**Триггеры:** «Где переплачиваем в РСЯ?», «Площадки с дорогими кликами без отдачи»

```sql
SELECT
    Placement, CampaignName,
    cpc, avg_cpc_campaign,
    round(cpc_deviation * 100, 0) AS cpc_overpay_pct,
    goal_score, cost, zone_status
FROM ym_sanok.bad_placements_v3
WHERE cpc_deviation > 0.5
  AND goal_score = 0
  AND zone_status != 'pending'
ORDER BY cpc_deviation DESC;
```

### 4. Площадки с высоким bounce_rate — некачественный трафик

**Триггеры:** «Где некачественный трафик в РСЯ?», «Высокий процент отказов»

```sql
SELECT
    Placement, CampaignName,
    clicks, bounce_rate, cost,
    goal_score, roas, zone_status
FROM ym_sanok.bad_placements_v3
WHERE bounce_rate > 70
  AND is_recent = 1
  AND clicks >= 10
ORDER BY bounce_rate DESC;
```

### 5. Площадки по конкретной кампании

**Триггеры:** «Площадки РСЯ в кампании X», «Где показывается реклама ванн?»

```sql
SELECT
    Placement, cost, clicks, roas,
    goal_score_rate, bench_goal_score_rate,
    bounce_rate, zone_status, zone_reason
FROM ym_sanok.bad_placements_v3
WHERE CampaignName ILIKE '%<название>%'
ORDER BY cost DESC;
```

### 6. Сводка по кампании: сколько красных, зелёных, жёлтых

**Триггеры:** «Как выглядит РСЯ в разрезе кампаний?», «Общий health-check площадок»

```sql
SELECT
    CampaignName,
    countIf(zone_status = 'red')     AS red,
    countIf(zone_status = 'yellow')  AS yellow,
    countIf(zone_status = 'green')   AS green,
    countIf(zone_status = 'pending') AS pending,
    round(sum(cost))                 AS total_cost,
    round(sum(purchase_revenue))     AS total_revenue
FROM ym_sanok.bad_placements_v3
WHERE is_recent = 1
GROUP BY CampaignName
ORDER BY total_cost DESC;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Плохие площадки / что исключить?» | `zone_status = 'red'`, ORDER BY cost DESC |
| «Лучшие площадки / масштабировать» | `zone_status = 'green'`, ORDER BY goal_score_rate DESC |
| «Где переплачиваем?» | `cpc_deviation > 0.5` |
| «Некачественный трафик?» | `bounce_rate > 70 AND is_recent = 1` |
| «Площадки без отдачи с расходом» | `goal_score = 0 AND cost > 400 AND is_recent = 1` |
| «Площадки кампании X» | `CampaignName ILIKE '%X%'` |
| «Почему площадка красная?» | `zone_reason` — код причины |
| «Площадка активна сейчас?» | `is_recent = 1` |
| «Health-check всего РСЯ» | GROUP BY CampaignName, COUNT по zone_status |
