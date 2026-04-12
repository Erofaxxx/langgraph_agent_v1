# Скоринг клиентов и ретаргетинг

## Система скоринга

```
dm_step_goal_impact
(какие цели работают)
        │
        ▼
dm_active_clients_scoring
(кто горячий и что делать)
```

Обновление: dm_step_goal_impact (06:00 UTC) → dm_active_clients_scoring (08:00 UTC).

## Таблица ym_sanok.dm_active_clients_scoring

Финальный продукт скоринга. Ежедневно оценивает каждого активного неконвертированного клиента (~104K): насколько близок к покупке, что делать, когда показать рекламу, и насколько дорогой товар он смотрит.

**Поля**: client_id, total_visits, last_visit_date, days_since_last, first_traffic_source, last_traffic_source, last_campaign, has_cart, has_checkout, lift_score, matched_goals, max_viewed_price, price_tier, priority (hot/warm/cold), next_step, recommended_goal_id, recommended_goal_name, recommended_lift, optimal_retarget_days, snapshot_date.

## Как вычисляется lift_score

1. Для каждого визита клиента (шаги 1-7) разворачиваем goalsID
2. Каждую пару (visit_number, goal_id) матчим с dm_step_goal_impact через INNER JOIN
3. Суммируем все lift'ы — это **lift_score**

Пример: клиент на 1-м визите открыл корзину (lift 56) + искал через поиск (lift 33) → lift_score = 89.

## Ценовой тир (price_tier)

| Тир | Условие | Диапазон |
|-----|---------|----------|
| low | max_viewed_price < 7 500 руб | ниже p25 |
| medium | 7 500 – 25 000 руб | p25–p75 |
| high | ≥ 25 000 руб | выше p75 |
| unknown | нет ценовых данных | ~66% клиентов |

Ценовой рычаг: клиент с lift_score=25 и дешёвым товаром → COLD. Тот же клиент с дорогим (≥7.5k) → WARM.

## Приоритеты

```
HOT  = (has_checkout=1 И визит ≤ 3 дней)
       ИЛИ (has_cart=1 И визит ≤ 7 дней)
       ИЛИ (lift_score > 50 И price_tier = 'high' И визит ≤ 3 дней)
       ИЛИ (lift_score > 80 И визит ≤ 3 дней)

WARM = (lift_score > 20 И price_tier ∈ {medium, high} И визит ≤ 10 дней)
       ИЛИ (lift_score > 30 И визит ≤ 7 дней)
       ИЛИ (has_cart=1 И визит ≤ 14 дней)

COLD = все остальные активные
```

Ожидаемое распределение: ~660 hot, ~1 590 warm, ~101 950 cold.

## Рекомендация (recommended_goal)

next_step = min(total_visits + 1, 7). Из dm_step_goal_impact берём цель с max lift на этом шаге (исключены транзакционные: 3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402).

## Тайминг (optimal_retarget_days)

| Шаг | Медиана gap | Дефолт |
|-----|-------------|--------|
| 2 | 3 дня | — |
| 3–7 | 2 дня | 2 дня |

## SQL-шаблоны

### Горячие клиенты (утренняя сводка)
```sql
SELECT client_id, total_visits, days_since_last, has_cart, has_checkout,
       round(lift_score, 1) AS lift_score, price_tier,
       round(max_viewed_price, 0) AS max_price,
       recommended_goal_name, optimal_retarget_days
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
  AND priority = 'hot'
ORDER BY lift_score DESC
LIMIT 50
```

### Клиенты, которых пора ретаргетить СЕГОДНЯ
```sql
SELECT client_id, priority, days_since_last, price_tier,
       round(optimal_retarget_days, 0) AS retarget_in,
       recommended_goal_name
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
  AND priority IN ('hot', 'warm')
  AND days_since_last BETWEEN toUInt16(round(optimal_retarget_days - 1))
                           AND toUInt16(round(optimal_retarget_days + 1))
ORDER BY lift_score DESC
```

### Почему клиент горячий — расшифровка скора
```sql
SELECT s.visit_number, s.goal_name, round(s.lift, 1) AS lift
FROM ym_sanok.visits AS v
ARRAY JOIN v.goalsID AS gid
INNER JOIN ym_sanok.dm_step_goal_impact AS s
    ON s.goal_id = gid
    AND s.visit_number = toUInt8(least(
        ROW_NUMBER() OVER (PARTITION BY v.clientID ORDER BY v.dateTime, v.visitID), 7))
WHERE v.clientID = <CLIENT_ID>
  AND s.snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
ORDER BY s.lift DESC
```

### Распределение по приоритетам (текущее)
```sql
SELECT priority, count() AS clients,
       countIf(has_checkout = 1) AS on_checkout,
       countIf(has_cart = 1) AS in_cart,
       countIf(price_tier = 'high') AS high_price,
       round(avg(lift_score), 1) AS avg_lift_score,
       round(avg(optimal_retarget_days), 1) AS avg_retarget_days
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
GROUP BY priority
ORDER BY CASE priority WHEN 'hot' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END
```

### Влияние цены на состав аудитории
```sql
SELECT priority, price_tier, count() AS clients,
       round(avg(lift_score), 1) AS avg_lift,
       round(avg(max_viewed_price), 0) AS avg_price
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
GROUP BY priority, price_tier
ORDER BY
    CASE priority WHEN 'hot' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END,
    CASE price_tier WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END
```

### Статистика по приоритетам (здоровье системы)
```sql
SELECT snapshot_date, priority, count() AS clients,
       round(avg(lift_score), 0) AS avg_score,
       countIf(price_tier = 'high') AS high_price_clients
FROM ym_sanok.dm_active_clients_scoring
GROUP BY snapshot_date, priority
ORDER BY snapshot_date DESC, CASE priority WHEN 'hot' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END
LIMIT 21
```
