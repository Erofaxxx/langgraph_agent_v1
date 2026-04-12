# bad_keywords_v1 — Ключевые фразы (поиск)

## Назначение

Ежедневный рейтинг ключевых фраз поисковых кампаний по эффективности. Для каждой фразы рассчитаны метрики за 60 дней, отклонение от медианы по кампании и автоматический вердикт (`zone_status`). Позволяет быстро находить ключи, которые тратят бюджет без отдачи, и ключи, которые конвертируют лучше остальных.

Типичные задачи: найти кандидатов на отключение или снижение ставки, выявить фразы для масштабирования, оценить давление аукциона на конкретный ключ.

## Источник данных и окно

- Таблица: `ym_sanok.direct_custom_report`
- Фильтр: `AdNetworkType = 'SEARCH'` и `CriterionType = 'KEYWORD'`
- Окно: **60 дней** скользящих (пересчитывается каждый день)
- Обновление: `REFRESH EVERY 1 DAY OFFSET 6 HOUR 30 MINUTE`

## Ключевые метрики

### goal_score — взвешенный балл конверсий

Суммирует конверсии с весами, отражающими близость к покупке. Веса рассчитаны по формуле `min(10, round(10 × order_paid / conversions_goal))` на данных 2025-09-01 – 2026-03-28 (order_paid = 1 136).

| Вес | Цели |
|-----|------|
| ×10 | Ecommerce: покупка · Целевой звонок · Уникальный звонок |
| ×8  | Jivo: чат начат клиентом · Заказ оформлен |
| ×5  | Оформление заказа · Заполнил контактные данные |
| ×2  | Добавление в корзину · Корзина |

`goal_score_rate = (goal_score / clicks) × 100` — конверсионность фразы на 100 кликов. Именно его сравнивают с медианой по кампании.

`tier12_conversions` — строгий счётчик: покупки + оба типа звонков (без весов). Используется как hard floor в зоне A.

### bid_zone — давление аукциона

`cpc_to_bid_ratio = фактический CPC / средняя ставка (взвешенная по кликам)`

| Зона | Соотношение | Смысл |
|------|-------------|-------|
| A | < 0.4 | Дешёвые клики — низкая конкуренция или нерелевантный трафик |
| B | 0.4 – 0.7 | Норма |
| C | 0.7 – 0.9 | Высокая конкуренция |
| D | > 0.9 | Клики почти по потолку ставки — перегретый аукцион |

### Отклонения от медианы кампании

Медиана считается по всем ключам **внутри одной кампании** (не по аккаунту):

- `goal_rate_deviation` — отклонение goal_score_rate: `0` = на уровне медианы, `-0.5` = вдвое хуже, `+0.3` = на 30% лучше
- `roas_deviation` — то же по ROAS (выручка / расход)
- При отсутствии конверсий/выручки принудительно `-1.0` (худший случай)

## Автоматический вердикт (zone_status)

Вердикт — стартовая точка, а не приговор. При нехватке данных алгоритм консервативно ставит `pending`.

### pending
Мало данных: `cost < 300` И `clicks < 20`. Делать выводы рано.

### green / yellow / red — логика зависит от bid_zone

Для зон C и D высокий CPC требует подтверждения конверсиями:

| bid_zone | green | yellow | red |
|----------|-------|--------|-----|
| D (>0.9) | goal_dev ≥ −0.2 И roas_dev ≥ −0.2 | одно из двух в норме | оба хуже порогов |
| C (0.7–0.9) | goal_dev ≥ −0.2 И roas_dev ≥ −0.3 | частичное соответствие | оба плохие |
| B (норма) | goal_dev ≥ −0.2 | goal_dev ≥ −0.5 ИЛИ roas_dev ≥ −0.2 | оба хуже |
| A (<0.4) | goal_dev ≥ −0.3 (и есть tier12 или расход небольшой) | goal_dev ≥ −0.6 | tier12=0 И cost > 500 |

## Когда zone_status можно пересмотреть

- **Брендовые фразы в red** — «санок» или «sanok» — стратегически важны вне зависимости от ROAS
- **Категорийные фразы в pending** — «ванны акриловые» в новой группе: 60 дней данных ещё нет
- **Зона A с нулём конверсий, но низкий расход** — алгоритм ставит red при cost > 500; при cost = 200 агент может оценить иначе
- **Сезонность** — летние/зимние пики по некоторым товарам могут исказить 60-дневное окно

В таких случаях смотреть на сырые метрики: `goal_score`, `tier12_conversions`, `cost`, `ctr`, `cpc_to_bid_ratio`.

## Структура таблицы

| Поле | Тип | Описание |
|------|-----|----------|
| report_date | Date | Дата расчёта |
| Criterion | String | Ключевая фраза |
| MatchType | String | Тип соответствия |
| CampaignId | UInt64 | ID кампании |
| CampaignName | String | Название кампании |
| AdGroupId | UInt64 | ID группы |
| AdGroupName | String | Название группы |
| clicks | UInt64 | Клики за 60 дней |
| impressions | UInt64 | Показы |
| cost | Float64 | Расход, руб |
| ctr | Float64 | CTR, % |
| cpc | Float64 | Фактический CPC, руб |
| avg_bid | Float64 | Средняя ставка (взвешенная по кликам) |
| cpc_to_bid_ratio | Float64 | CPC / ставка — основа bid_zone |
| purchase_revenue | Float64 | Выручка (атрибуция Директа) |
| roas | Float64 | Выручка / расход |
| goal_score | Float64 | Взвешенный балл конверсий |
| goal_score_rate | Float64 | goal_score на 100 кликов |
| tier12_conversions | UInt64 | Покупки + звонки (без весов) |
| goal_rate_deviation | Float64 | Отклонение goal_score_rate от медианы кампании |
| roas_deviation | Float64 | Отклонение ROAS от медианы кампании |
| med_goal_score_rate | Float64 | Медиана goal_score_rate по кампании |
| med_roas | Float64 | Медиана ROAS по кампании |
| bid_zone | String | A / B / C / D |
| zone_status | String | green / yellow / red / pending |

## Сценарии использования для AI-агента

### 1. Красные ключи с большим расходом — срочные к проверке

**Триггеры:** «Какие ключи сжигают бюджет?», «Найди плохие ключевые фразы», «Что отключить?»

```sql
SELECT
    Criterion, MatchType, CampaignName, AdGroupName,
    clicks, cost, tier12_conversions,
    goal_score_rate, med_goal_score_rate,
    bid_zone, zone_status
FROM ym_sanok.bad_keywords_v1
WHERE zone_status = 'red'
ORDER BY cost DESC
LIMIT 30;
```

### 2. Зелёные ключи — кандидаты на повышение ставок

**Триггеры:** «Где можно увеличить ставку?», «Какие ключи работают лучше всего?», «Точки роста»

```sql
SELECT
    Criterion, CampaignName, AdGroupName,
    clicks, cost, tier12_conversions,
    goal_score_rate, med_goal_score_rate,
    round(goal_rate_deviation * 100, 0) AS deviation_pct,
    bid_zone, cpc, avg_bid
FROM ym_sanok.bad_keywords_v1
WHERE zone_status = 'green'
  AND goal_rate_deviation > 0.3
ORDER BY tier12_conversions DESC, goal_score_rate DESC;
```

### 3. Зона D — перегретый аукцион, платим максимум

**Триггеры:** «Где мы переплачиваем?», «Перегретые ключи», «Где снизить ставку?»

```sql
SELECT
    Criterion, CampaignName, cost, cpc, avg_bid,
    round(cpc_to_bid_ratio, 2) AS cpc_bid_ratio,
    tier12_conversions, goal_score_rate, zone_status
FROM ym_sanok.bad_keywords_v1
WHERE bid_zone = 'D'
ORDER BY cost DESC;
```

### 4. Ключи без конверсий с накопленным расходом

**Триггеры:** «Ключи с расходом без отдачи», «Где нет ни одной конверсии?»

```sql
SELECT
    Criterion, CampaignName, AdGroupName,
    clicks, cost, goal_score, tier12_conversions,
    bid_zone, zone_status
FROM ym_sanok.bad_keywords_v1
WHERE tier12_conversions = 0
  AND cost > 500
  AND zone_status != 'pending'
ORDER BY cost DESC;
```

### 5. Ключи по конкретной кампании или группе

**Триггеры:** «Покажи ключи кампании X», «Как работают ключи группы Y?»

```sql
SELECT
    Criterion, MatchType,
    clicks, cost, tier12_conversions,
    goal_score_rate, med_goal_score_rate,
    bid_zone, zone_status
FROM ym_sanok.bad_keywords_v1
WHERE CampaignName ILIKE '%<название>%'
ORDER BY cost DESC;
```

### 6. Сравнение типов соответствия

**Триггеры:** «Точное vs широкое соответствие — что эффективнее?», «Как MatchType влияет на конверсии?»

```sql
SELECT
    MatchType,
    count()                                 AS keywords,
    sum(cost)                               AS total_cost,
    sum(tier12_conversions)                 AS conversions,
    round(avg(goal_score_rate), 2)          AS avg_gsr,
    countIf(zone_status = 'red')            AS red_count,
    countIf(zone_status = 'green')          AS green_count
FROM ym_sanok.bad_keywords_v1
GROUP BY MatchType
ORDER BY total_cost DESC;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Плохие / красные ключи» | `zone_status = 'red'`, ORDER BY cost DESC |
| «Лучшие ключи / зелёные» | `zone_status = 'green'`, ORDER BY goal_score_rate DESC |
| «Ключи без конверсий с расходом» | `tier12_conversions = 0 AND cost > 300` |
| «Где переплачиваем?» | `bid_zone = 'D'` |
| «Давление аукциона» | `cpc_to_bid_ratio`, `bid_zone` |
| «Ключи без данных / pending» | `zone_status = 'pending'` |
| «Ключи кампании X» | `CampaignName ILIKE '%X%'` |
| «Насколько хуже/лучше медианы?» | `goal_rate_deviation`, `roas_deviation` |
| «Какие цели засчитываются ключу?» | JOIN `goal_dict` на нужный goal_id |
| «Сравнить типы соответствия» | `GROUP BY MatchType` |
