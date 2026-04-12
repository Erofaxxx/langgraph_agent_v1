# dm_active_clients_scoring — Sanok

## Назначение

Финальный продукт системы скоринга. Ежедневно оценивает каждого активного неконвертированного клиента: насколько он близок к покупке, что ему показать, когда показать и насколько дорогой товар он смотрит.

Готовый таргет-лист для офферного ретаргетинга: кому, когда, какую цель стимулировать.

## Ключевое отличие от magnetto

В sanok к поведенческому скору добавлено **ценовое измерение**: клиент с умеренным lift_score, который смотрит дорогие товары (≥25k руб), получает более высокий приоритет. Логика: высокий чек оправдывает агрессивный ретаргетинг даже при неполном поведенческом сигнале.

## Источники данных

| Источник | Что берём |
|---|---|
| `dm_client_profile` | Профиль: has_purchased, визиты, utm-источники |
| `dm_step_goal_impact` | Lift по целям для расчёта lift_score и рекомендаций |
| `visits` | goalsID → lift_score; productID → цены товаров |
| `dm_products3` | avg_unit_price по product_id → price_tier |

## Обновление

`REFRESH EVERY 1 DAY OFFSET 8 HOUR` — после `dm_step_goal_impact` (06:00 UTC).

```
ym_sanok.visits (near real-time)
    │
    ├─► dm_client_profile_mv   (INSERT-triggered)
    │
    └─► dm_step_goal_impact_mv REFRESH 06:00 UTC
              │
              ▼
    dm_active_clients_scoring_mv REFRESH 08:00 UTC
```

## Активное окно

Клиенты с `has_purchased = 0` и `last_visit_date >= today() - 60`.

60 дней выбрано по p90 цикла покупки sanok = 63 дня (медиана = 7 дней, p75 = 25 дней). Текущий объём: **~104 000 клиентов**.

## Как работает (полная логика)

### Шаг 1: Активные неконвертированные клиенты

```sql
WHERE has_purchased = 0 AND last_visit_date >= today() - 60
```

### Шаг 2: Нумерация визитов и расчёт lift_score

Для каждого активного клиента:
1. Берём все его исторические визиты из `visits` (не только последние 60 дней — нужен полный порядок для корректного visit_number)
2. Нумеруем через `ROW_NUMBER() OVER (PARTITION BY clientID ORDER BY dateTime, visitID)`
3. Разворачиваем `goalsID` через `arrayJoin`
4. Матчим пары (visit_number, goal_id) с `dm_step_goal_impact`
5. `lift_score` = сумма всех совпавших lift'ов

```
Пример: клиент на 1-м визите открыл корзину (lift 56) + искал через поиск (lift 33).
lift_score = 56 + 33 = 89.
```

`matched_goals` — количество совпавших пар. Чем больше — тем надёжнее скор.

### Шаг 3: Флаги корзины и чекаута

Из визитов за **последние 60 дней**:

| Флаг | goal_id | Название |
|------|---------|----------|
| `has_checkout` | 21115915 | Оформление заказа (/simplecheckout) |
| `has_checkout` | 495725161 | Автоцель: начало оформления заказа |
| `has_cart` | 21115645 | Корзина (/cart) |
| `has_cart` | 194388760 | Ecommerce: добавление в корзину |

> `has_checkout` — клиент был на странице оформления заказа, но НЕ завершил покупку (иначе он уже не был бы в активных). Это сильнейший сигнал: lift = 96 на шаге 1.

### Шаг 4: Ценовой тир

Из визитов за **последние 60 дней**: `productID[]` → JOIN `dm_products3.avg_unit_price` → `max_viewed_price`.

Пороги (из реального распределения dm_products3):

| Тир | Условие | Диапазон |
|-----|---------|----------|
| `low` | max_viewed_price < 7 500 руб | ниже p25 |
| `medium` | 7 500 – 25 000 руб | p25–p75 |
| `high` | ≥ 25 000 руб | выше p75 |
| `unknown` | нет ценовых данных | ~34% просмотров имеют цену |

> **Почему 34%?** `dm_products3.avg_unit_price` рассчитывается из истории продаж. Товары, которые просматривали но не покупали, не попадают в dm_products3. При наполнении базы покрытие будет расти.

### Шаг 5: Приоритет

```
HOT  = (has_checkout = 1  И  визит ≤ 3 дней)
       ИЛИ (has_cart = 1  И  визит ≤ 7 дней)
       ИЛИ (lift_score > 50  И  price_tier = 'high'  И  визит ≤ 3 дней)
       ИЛИ (lift_score > 80  И  визит ≤ 3 дней)

WARM = (lift_score > 20  И  price_tier ∈ {'medium','high'}  И  визит ≤ 10 дней)
       ИЛИ (lift_score > 30  И  визит ≤ 7 дней)
       ИЛИ (has_cart = 1  И  визит ≤ 14 дней)

COLD = все остальные активные
```

**Смысл ценового рычага**: клиент с lift_score = 25 и дешёвым товаром → COLD. Тот же клиент с дорогим товаром (≥7.5k) → WARM. Цена усиливает сигнал, потому что высокий чек оправдывает рекламный контакт.

### Шаг 6: Рекомендация

`next_step = min(total_visits + 1, 7)`

Из `dm_step_goal_impact` берём цель с максимальным lift на этом шаге (исключены транзакционные: 3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402).

Поле `recommended_goal_id` всегда присутствует рядом с `recommended_goal_name` — для обратной сверки с `goals_sanok.md`.

### Шаг 7: Тайминг

`optimal_retarget_days` = медиана gap между визитами у покупателей на `next_step`.

Реальные значения:

| Шаг | Медиана gap | Дефолт |
|-----|-------------|--------|
| 2 | 3 дня | — |
| 3–7 | 2 дня | 2 дня |

## Структура таблицы

| Поле | Тип | Описание |
|------|-----|----------|
| client_id | UInt64 | ID клиента Метрики |
| total_visits | UInt32 | Всего визитов за всё время |
| last_visit_date | Date | Дата последнего визита |
| days_since_last | UInt16 | Дней с последнего визита |
| first_traffic_source | String | Первый UTM source (first_utm_source) |
| last_traffic_source | String | Последний UTM source (last_utm_source) |
| last_campaign | String | Последняя UTM campaign |
| has_cart | UInt8 | 1 = был в корзине/добавлял за 60 дней (goals 21115645, 194388760) |
| has_checkout | UInt8 | 1 = был на чекауте за 60 дней (goals 21115915, 495725161) |
| lift_score | Float32 | Сумма lift'ов по достигнутым целям |
| matched_goals | UInt16 | Кол-во совпавших (visit_number × goal_id) |
| max_viewed_price | Float32 | Макс avg_unit_price из просмотренных товаров (60 дней) |
| price_tier | String | low / medium / high / unknown |
| priority | String | **hot / warm / cold** |
| next_step | UInt8 | Следующий визит (capped 7) |
| recommended_goal_id | UInt32 | goal_id цели с макс. lift на next_step (см. goals_sanok.md) |
| recommended_goal_name | String | Название рекомендованной цели |
| recommended_lift | Float32 | Ожидаемый lift рекомендации |
| optimal_retarget_days | Float32 | Через сколько дней показать рекламу |
| snapshot_date | Date | Дата пересчёта |

`ORDER BY (priority, client_id)` — быстрые запросы по приоритету.

## Ожидаемое распределение (при свежих данных)

| Приоритет | Клиентов | Ср. lift | Ср. цена | high-price |
|-----------|----------|----------|----------|------------|
| hot | ~660 | ~184 | ~15 500 руб | ~145 |
| warm | ~1 590 | ~117 | ~17 000 руб | ~380 |
| cold | ~101 950 | ~11 | ~5 600 руб | ~7 750 |

> **Примечание о текущем состоянии**: данные Метрики в базе обновлены по 28.03.2026. До загрузки свежих данных все клиенты отображаются как cold (min days_since_last = 15). Логика приоритетов верна — система оживёт при следующем импорте.

## Сценарии использования для AI-агента

### 1. Кого ретаргетировать сегодня

**Триггеры**: "Покажи горячих клиентов", "Кому показать рекламу сегодня?", "Список для ретаргетинга"

```sql
SELECT
    client_id,
    total_visits,
    days_since_last,
    has_cart,
    has_checkout,
    round(lift_score, 1)      AS lift_score,
    price_tier,
    round(max_viewed_price, 0) AS max_price,
    recommended_goal_id,
    recommended_goal_name,
    round(recommended_lift, 1) AS rec_lift,
    optimal_retarget_days
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
  AND priority = 'hot'
ORDER BY lift_score DESC
```

### 2. Клиенты в оптимальном окне для контакта прямо сейчас

**Триггеры**: "Кто готов к рекламному контакту именно сейчас?", "Кому пора показать рекламу?"

```sql
SELECT
    client_id,
    priority,
    days_since_last,
    round(optimal_retarget_days, 0) AS retarget_in,
    recommended_goal_id,
    recommended_goal_name,
    price_tier,
    round(lift_score, 1) AS lift_score
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
  AND priority IN ('hot', 'warm')
  AND days_since_last BETWEEN toUInt16(round(optimal_retarget_days - 1))
                          AND toUInt16(round(optimal_retarget_days + 1))
ORDER BY lift_score DESC
```

### 3. Почему клиент получил этот приоритет

**Триггеры**: "Почему этот клиент в hot?", "Расшифруй скор клиента X", "Что он делал на сайте?"

```sql
-- Подставить реальный client_id
SELECT
    s.visit_number,
    s.goal_id,
    s.goal_name,
    round(s.lift, 1) AS lift
FROM ym_sanok.visits AS v
ARRAY JOIN v.goalsID AS gid
INNER JOIN ym_sanok.dm_step_goal_impact AS s
    ON  s.goal_id      = gid
    AND s.visit_number = toUInt8(least(
        ROW_NUMBER() OVER (PARTITION BY v.clientID ORDER BY v.dateTime, v.visitID),
        7
    ))
WHERE v.clientID = <CLIENT_ID>
  AND s.snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
ORDER BY s.lift DESC
```

### 4. Влияние цены на состав аудитории

**Триггеры**: "Сколько дорогих клиентов в hot?", "Цена помогает приоритету?", "Кто смотрит дорогие товары?"

```sql
SELECT
    priority,
    price_tier,
    count()                          AS clients,
    round(avg(lift_score), 1)        AS avg_lift,
    round(avg(max_viewed_price), 0)  AS avg_price
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
GROUP BY priority, price_tier
ORDER BY
    CASE priority WHEN 'hot' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END,
    CASE price_tier WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END
```

### 5. Сводка по ретаргетингу — утренний брифинг

**Триггеры**: "Что сегодня?", "Сводка по рекламе", "Кого ретаргетить и что показывать?"

```sql
SELECT
    priority,
    count()                               AS clients,
    countIf(has_checkout = 1)             AS on_checkout,
    countIf(has_cart = 1)                 AS in_cart,
    countIf(price_tier = 'high')          AS high_price,
    round(avg(lift_score), 1)             AS avg_lift_score,
    argMax(recommended_goal_id,   lift_score) AS top_goal_id,
    argMax(recommended_goal_name, lift_score) AS top_goal_name,
    round(avg(optimal_retarget_days), 1)  AS avg_retarget_days
FROM ym_sanok.dm_active_clients_scoring
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_active_clients_scoring)
GROUP BY priority
ORDER BY CASE priority WHEN 'hot' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END
```

### 6. Динамика скоринга — здоровье системы

**Триггеры**: "Данные обновились?", "Сколько горячих сегодня vs вчера?", "Динамика приоритетов"

```sql
SELECT
    snapshot_date,
    priority,
    count()                      AS clients,
    round(avg(lift_score), 0)    AS avg_score,
    countIf(price_tier = 'high') AS high_price_clients
FROM ym_sanok.dm_active_clients_scoring
GROUP BY snapshot_date, priority
ORDER BY snapshot_date DESC,
         CASE priority WHEN 'hot' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END
LIMIT 21
```

## Технические уточнения

**Архитектурный нюанс ClickHouse 26.3**: нельзя использовать `FROM cte_name` в финальном SELECT когда CTE транзитивно зависит от других CTE через WITH. Поэтому все JOIN'ы вынесены в финальный SELECT. `price_tier` вычислен дважды: как явное поле и инлайн внутри `priority multiIf` через `cp.max_viewed_price >= N`.

**Нумерация визитов**: используем `ROW_NUMBER()` напрямую из `visits`, не из `dm_client_journey`. Причина: INSERT-triggered MV вычисляет `ROW_NUMBER()` в рамках каждого батча вставки, поэтому для повторных клиентов visit_number в `dm_client_journey` может быть занижен. В REFRESHABLE MV `ROW_NUMBER()` обрабатывает всю таблицу разом — результат корректен.

**Ценовые данные**: `dm_products3.avg_unit_price` присутствует только для товаров с историей продаж (~34% просмотров). Клиенты, которые просматривали только непроданные товары, получают `price_tier = 'unknown'` и не получают ценового буста приоритета. Это консервативно и корректно.

**has_cart и has_checkout** — смотрим только за последние 60 дней (активное окно), а не за всё время клиента.

**recommended_goal_id** всегда присутствует рядом с `recommended_goal_name`. При использовании названия цели — проверяй по `goals_sanok.md` через goal_id.
