# bad_queries_v3 — Поисковые запросы

## Назначение

Ежедневный рейтинг реальных поисковых запросов, по которым Яндекс показывал рекламу sanok. В отличие от `bad_keywords_v1` (фразы, которые мы добавили сами), здесь — то, что реально вводили пользователи. Позволяет находить нецелевые запросы для минус-слов, выявлять хронические источники пустого трафика, обнаруживать запросы-жемчужины для расширения семантики.

Типичные задачи: еженедельная чистка минус-слов, анализ качества автотаргетинга, поиск новых точных ключевых фраз из реального поиска.

## Источник данных и окно

- Таблица: `ym_sanok.direct_search_queries_goals`
- Окно: **180 дней** (в 3 раза больше, чем у keywords и placements — запросы накапливают данные медленнее, нужно больше истории для надёжного вердикта)
- Фильтр: только записи с `Clicks > 0`
- Обновление: `REFRESH EVERY 1 DAY OFFSET 7 HOUR`

> **Важно:** источник `direct_search_queries_goals` содержит только 7 conversion-колонок (против 9+ в `direct_custom_report`). Недоступны: call_unique, call_targeted, jivo, filled_contacts. Звонки представлены только через `Conversions_34740969` (Звонок Calltouch).

## Ключевые метрики

### goal_score — взвешенный балл конверсий

Адаптирован под доступные колонки источника:

| Вес | Цель |
|-----|------|
| ×10 | Ecommerce: покупка |
| ×10 | Звонок Calltouch (единственный доступный звонок в источнике) |
| ×8  | Заказ оформлен |
| ×7  | Автоцель: начало оформления заказа |
| ×5  | Оформление заказа |
| ×2  | Добавление в корзину |
| ×2  | Корзина |

`goal_score_rate = goal_score / clicks` — конверсионность запроса на клик (не умножается на 100 — масштаб отличается от bad_keywords).

### is_chronic и is_recent

- `is_recent = 1` — запрос показывался в последние 20 дней (актуален)
- `is_chronic = 1` — запрос активен 14+ дней (`days_active >= 14`): появляется систематически, а не разово
- `days_active` — количество уникальных дней с хотя бы одним кликом

**Хронический нецелевой запрос** (`is_chronic = 1`, `goal_score = 0`, `is_recent = 1`) — первый кандидат в минус-слова.

### matched_keyword
Ключевая фраза, с которой сматчился запрос. Помогает понять: это проблема конкретного ключа (слишком широкое соответствие) или системная проблема всей группы.

### Бенчмарки по кампании

- `bench_roas` — средний ROAS по кампании (взвешенный по кликам)
- `bench_goal_score` — средний goal_score на клик по кампании
- `goal_rate_deviation` — отклонение от бенчмарка: `-1.0` если конверсий нет
- `roas_deviation` — отклонение ROAS: `-1.0` если выручки нет

## Автоматический вердикт (zone_status + zone_reason)

### pending
Запрос неактуален (`is_recent = 0`) или данных мало (`clicks < 5` или `cost < 200`).

### green — конвертирующий запрос
`ROAS > 2` — прямая окупаемость.

`zone_reason`: `g:roas>2`

### red — нецелевой запрос, добавить в минус-слова
Любое из условий:
- Отказов > 90% И нет выручки — явно нецелевая аудитория
- Отказов > 60% И нет выручки И goal_rate_deviation < −0.5 — нецелевой трафик с большим отрывом хуже медианы
- Нет конверсий вообще И расход > 400 руб

`zone_reason`: `r:bounce>90+no_roas` · `r:bounce>60+no_roas+gdev<-0.5` · `r:no_goals+cost>400`

### yellow — неоднозначный запрос
Не конвертирует напрямую, но и явных сигналов мусора нет. Требует контекстного суждения по смыслу запроса.

## Когда zone_status можно пересмотреть

- **Информационные запросы в yellow** — «какая ванна лучше», «размеры душевой кабины» — не конвертируют напрямую, но это аудитория на стадии выбора; в минус не стоит
- **Запросы конкурентов в yellow/red** — «сантехника в леруа», «ванны в икеа» — в минус безусловно, даже если cost < 400
- **Брендовые запросы sanok** — «sanok.ru», «магазин санок» — никогда в минус
- **Хронический red с малым расходом** — `is_chronic = 1`, cost = 80 руб — алгоритм ставит pending (cost < 200), но это систематическая проблема, на которую стоит обратить внимание
- **Запросы через автотаргетинг** — CriterionType = 'AUTOTARGETING': нормативы применимы, но контекст другой — смотреть на TargetingCategory

В таких случаях агент смотрит на `Query`, `matched_keyword`, `bounce_rate`, `days_active`, `is_chronic` и выносит суждение самостоятельно.

## Структура таблицы

| Поле | Тип | Описание |
|------|-----|----------|
| report_date | Date | Дата расчёта |
| Query | String | Реальный поисковый запрос пользователя |
| CriterionType | String | Тип таргетинга (KEYWORD / AUTOTARGETING и др.) |
| TargetingCategory | String | Категория автотаргетинга (если применимо) |
| CampaignId | UInt64 | ID кампании |
| CampaignName | String | Название кампании |
| matched_keyword | String | Ключ, с которым сматчился запрос |
| clicks | UInt64 | Клики за 180 дней |
| impressions | UInt64 | Показы |
| cost | Float64 | Расход, руб |
| ctr | Nullable(Float64) | CTR, % |
| cpc | Nullable(Float64) | CPC, руб |
| bounce_rate | Nullable(Float64) | Доля отказов, % |
| days_active | UInt64 | Дней с хотя бы одним кликом |
| is_chronic | UInt8 | 1 = активен 14+ дней |
| is_recent | UInt8 | 1 = активен в последние 20 дней |
| purchase_revenue | Float64 | Выручка (атрибуция Директа) |
| roas | Nullable(Float64) | Выручка / расход |
| goal_score | Float64 | Взвешенный балл конверсий |
| goal_score_rate | Nullable(Float64) | goal_score на клик |
| goal_rate_deviation | Nullable(Float64) | Отклонение от бенчмарка кампании |
| roas_deviation | Nullable(Float64) | Отклонение ROAS от бенчмарка |
| bench_roas | Nullable(Float64) | Средний ROAS по кампании |
| bench_goal_score | Nullable(Float64) | Средний goal_score на клик по кампании |
| zone_status | String | green / yellow / red / pending |
| zone_reason | String | Машиночитаемая причина вердикта |

## Сценарии использования для AI-агента

### 1. Красные запросы — кандидаты в минус-слова

**Триггеры:** «Какие запросы добавить в минус-слова?», «Нецелевые поисковые запросы», «Что чистить?»

```sql
SELECT
    Query, matched_keyword, CampaignName,
    clicks, cost, bounce_rate,
    goal_score, days_active, is_chronic,
    zone_reason
FROM ym_sanok.bad_queries_v3
WHERE zone_status = 'red'
ORDER BY cost DESC
LIMIT 30;
```

### 2. Хронические нецелевые запросы — системная проблема

**Триггеры:** «Есть ли запросы, которые постоянно тратят бюджет?», «Хронические минус-слова»

```sql
SELECT
    Query, matched_keyword, CampaignName,
    days_active, clicks, cost,
    goal_score, bounce_rate, zone_status
FROM ym_sanok.bad_queries_v3
WHERE is_chronic = 1
  AND goal_score = 0
  AND is_recent = 1
ORDER BY cost DESC;
```

### 3. Зелёные запросы — добавить как точные ключевые фразы

**Триггеры:** «Какие запросы хорошо работают?», «Запросы для расширения семантики», «Что добавить в ключи?»

```sql
SELECT
    Query, matched_keyword, CampaignName,
    clicks, cost, roas, goal_score,
    goal_score_rate, bench_goal_score,
    round(goal_rate_deviation * 100, 0) AS deviation_pct,
    zone_reason
FROM ym_sanok.bad_queries_v3
WHERE zone_status = 'green'
ORDER BY goal_score DESC;
```

### 4. Запросы с высоким bounce_rate

**Триггеры:** «Запросы с плохим качеством трафика», «Где нецелевая аудитория?»

```sql
SELECT
    Query, matched_keyword, CampaignName,
    clicks, bounce_rate, cost,
    goal_score, zone_status
FROM ym_sanok.bad_queries_v3
WHERE bounce_rate > 70
  AND is_recent = 1
  AND clicks >= 5
ORDER BY bounce_rate DESC;
```

### 5. Запросы через автотаргетинг — что притягивает

**Триггеры:** «Какие запросы приходят через автотаргетинг?», «Что матчит автотаргетинг?»

```sql
SELECT
    Query, TargetingCategory, CampaignName,
    clicks, cost, bounce_rate,
    goal_score, zone_status, zone_reason
FROM ym_sanok.bad_queries_v3
WHERE CriterionType = 'AUTOTARGETING'
  AND is_recent = 1
ORDER BY cost DESC;
```

### 6. Что реально ищут по конкретному ключу

**Триггеры:** «Какие запросы сматчились с ключом "ванны акриловые"?», «Что ищут по фразе X?»

```sql
SELECT
    Query, clicks, cost, bounce_rate,
    goal_score, zone_status, zone_reason
FROM ym_sanok.bad_queries_v3
WHERE matched_keyword ILIKE '%<ключ>%'
ORDER BY cost DESC;
```

### 7. Сводка по кампании

**Триггеры:** «Какой трафик идёт в кампанию X?», «Health-check семантики кампании»

```sql
SELECT
    zone_status,
    count()                          AS queries,
    sum(cost)                        AS total_cost,
    sum(goal_score)                  AS total_gs,
    round(avg(bounce_rate), 1)       AS avg_bounce
FROM ym_sanok.bad_queries_v3
WHERE CampaignName ILIKE '%<название>%'
  AND is_recent = 1
GROUP BY zone_status
ORDER BY total_cost DESC;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Минус-слова / нецелевые запросы» | `zone_status = 'red'`, ORDER BY cost DESC |
| «Хронические проблемы» | `is_chronic = 1 AND goal_score = 0 AND is_recent = 1` |
| «Запросы для расширения семантики» | `zone_status = 'green'` |
| «Высокий bounce по запросам» | `bounce_rate > 70 AND is_recent = 1` |
| «Автотаргетинг что притягивает?» | `CriterionType = 'AUTOTARGETING'` |
| «Что ищут по ключу X?» | `matched_keyword ILIKE '%X%'` |
| «Запросы кампании X» | `CampaignName ILIKE '%X%'` |
| «Почему запрос красный?» | `zone_reason` — код причины |
| «Сколько дней запрос активен?» | `days_active`, `is_chronic` |
| «Запрос встречается системно?» | `is_chronic = 1 AND days_active >= 30` |
