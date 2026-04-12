# dm_step_goal_impact — Sanok

## Назначение

Фундамент системы скоринга покупателей. Для каждой пары **(номер визита × цель Метрики)** вычисляет lift — во сколько раз клиент, выполнивший эту цель на данном шаге, конвертируется в покупку чаще, чем клиент без этой цели.

Результат — числовая оценка ценности каждого поведенческого сигнала на каждом этапе пути покупателя. Используется в `dm_active_clients_scoring` (следующая витрина) для ранжирования активных клиентов.

## Чем отличается от magnetto.dm_step_goal_impact

| Параметр | Magnetto | Sanok |
|---|---|---|
| Конверсия | CRM-сделка (`has_crm_created = 1`) | Покупка (`has_purchased = 1`) |
| Базовая конверсия | ~0.09% | ~1.19% |
| Шаги | 1–10 | **1–7** (медиана покупателя = 3 визита) |
| Мин. выборка | 20 клиентов | **50 клиентов** (конверсия выше, нужно меньше) |
| Источник visit_number | `dm_client_journey.visit_number` | **ROW_NUMBER() из `visits`** (INSERT-triggered MV даёт неточный номер для повторных клиентов) |

## Источники данных

- `ym_sanok.visits` — goalsID (цели визита), нумерация визитов через ROW_NUMBER() по (clientID, dateTime, visitID)
- `ym_sanok.dm_client_profile` — `has_purchased = 1` (определение конвертера)

## Обновление

`REFRESH EVERY 1 DAY OFFSET 6 HOUR` — полный пересчёт ежедневно в 06:00 UTC.

`dm_client_profile` и `visits` обновляются в реальном времени через INSERT-triggered MV, поэтому отдельного ожидания не нужно.

## Логика вычислений

### Шаг 1: Конвертеры

```sql
SELECT client_id FROM ym_sanok.dm_client_profile WHERE has_purchased = 1
-- ~4 961 клиент из 417 410 (1.19%)
```

### Шаг 2: Нумерация визитов

```sql
ROW_NUMBER() OVER (PARTITION BY clientID ORDER BY dateTime ASC, visitID ASC)
```

`REFRESHABLE MV` обрабатывает все данные разом — ROW_NUMBER корректен. В INSERT-triggered MV нумерация была бы неточна для повторных клиентов (MV видит только текущий батч INSERT'а).

### Шаг 3: Развёртка goalsID

`arrayJoin(goalsID)` → одна строка на каждую достигнутую цель в визите.

### Шаг 4: goal_stats — группа «с целью»

Для каждой пары (visit_number, goal_id):
- `clients_with_goal` — уникальных клиентов, выполнивших цель
- `converters_with_goal` — из них совершили покупку

Фильтр: `HAVING clients_with_goal >= 50` — статистическая значимость.

### Шаг 5: step_baseline — базовая линия

Для каждого шага: всего клиентов и конвертеров.

### Шаг 6: Вычисляем lift

```
rate_with_goal    = converters_with_goal / clients_with_goal
rate_without_goal = (converters_at_step - converters_with_goal)
                    / (clients_at_step - clients_with_goal)
lift              = rate_with_goal / rate_without_goal
```

**Lift 99** означает: клиент, заполнивший контактные данные на 1-м визите, покупает в 99 раз чаще среднего.

## Структура таблицы

| Поле | Тип | Описание |
|------|-----|----------|
| visit_number | UInt8 | Номер визита (1–7) |
| goal_id | UInt32 | ID цели Метрики (справочник: goals_sanok.md) |
| goal_name | String | Название цели. Если ID не в маппинге — toString(goal_id) |
| clients_at_step | UInt32 | Всего уникальных клиентов, дошедших до этого шага |
| clients_with_goal | UInt32 | Из них выполнили цель на этом шаге |
| clients_without_goal | UInt32 | Не выполнили |
| converters_with_goal | UInt32 | Покупатели среди выполнивших цель |
| converters_without_goal | UInt32 | Покупатели среди НЕ выполнивших |
| rate_with_goal | Float32 | Конверсия группы «с целью» |
| rate_without_goal | Float32 | Конверсия группы «без цели» |
| lift | Float32 | rate_with / rate_without (>1 = цель помогает) |
| snapshot_date | Date | Дата последнего пересчёта |

`ORDER BY (visit_number, goal_id)` — быстрый поиск по шагу и цели.

Текущий объём: **279 строк** (7 шагов × ~40 значимых целей).

## Тавтологические цели — ИСКЛЮЧАТЬ из рекомендаций

Эти цели имеют высокий lift, но срабатывают ПОСЛЕ или ВО ВРЕМЯ покупки — тавтология. В таблице присутствуют (для прозрачности), но в `dm_active_clients_scoring` исключаются при формировании рекомендаций.

| goal_id | Название | Почему исключать |
|---------|----------|-----------------|
| 3000178943 | Ecommerce: покупка | Цель = покупка, lift = тавтология |
| 31297300 | Заказ оформлен | URL /checkout/success = после покупки |
| 543662393 | Успешное оформление заказа | Составная цель по факту покупки |
| 543662395 | Заказ с предоплатой | После покупки |
| 543662396 | Заказ с постоплатой | После покупки |
| 543662401 | Отмененный заказ | Шум (негативный сигнал) |
| 543662402 | Заказ доставлен | После покупки |

## Реальные данные: топ целей после первого рефреша

Базовая конверсия: ~1.19%. Исключены тавтологии.

**Шаг 1 (первый визит) — самые сильные сигналы:**

| goal_id | Название | Conv % (с целью) | Lift |
|---------|----------|-----------------|------|
| 317172960 | Автоцель: заполнил контактные данные | 55.5% | **99** |
| 21115915 | Оформление заказа | 53.3% | **96** |
| 21115645 | Корзина | 27.9% | **56** |
| 194388760 | Ecommerce: добавление в корзину | 24.2% | **55** |
| 495725161 | Автоцель: начало оформления заказа | 36.6% | **41** |
| 339159404 | Автоцель: отправил контактные данные | 38.9% | **35** |
| 175941409 | Автоцель: поиск по сайту | 37.6% | **33** |
| 157742788 | Более 3-х страниц | 4.8% | **12** |
| 47284972 | Jivo: заполнил форму контактов | 13.1% | **11** |

> Примечание: `Оформление заказа` (21115915) — это страница `/simplecheckout`, которую клиент посещает ДО завершения покупки. Это сильный предиктор, не тавтология.

> `Поиск по сайту` (175941409) — неожиданно сильный сигнал (lift 33). Клиенты, которые используют поиск, знают, что ищут.

**Шаг 2 — lift снижается, структура сохраняется:**

| goal_id | Название | Lift |
|---------|----------|------|
| 21115915 | Оформление заказа | 22 |
| 317172960 | Заполнил контактные данные | 21 |
| 495725161 | Начало оформления заказа | 14 |
| 21115645 | Корзина | 14 |

## Сценарии использования для AI-агента

### 1. Какие цели стимулировать в рекламе

**Триггеры**: "На какие действия нацелить рекламу?", "Какие цели реально влияют на покупки?", "Что должен сделать клиент на сайте?"

```sql
SELECT
    visit_number,
    goal_id,
    goal_name,
    round(lift, 1)            AS lift,
    clients_with_goal,
    converters_with_goal,
    round(rate_with_goal * 100, 2) AS conv_pct
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_id NOT IN (3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402)
  AND lift > 5
ORDER BY lift DESC
```

**Интерпретация**: топ целей — это то, к чему нужно подталкивать клиента. Если "Корзина" имеет lift 56 — рекламные объявления должны вести на конкретные товары с кнопкой "В корзину", а не на главную страницу.

### 2. Работает ли конкретный инструмент

**Триггеры**: "Jivo помогает продажам?", "Есть ли смысл в поп-апах?", "Поиск по сайту конвертирует?"

```sql
SELECT
    visit_number,
    goal_id,
    goal_name,
    round(lift, 1) AS lift,
    clients_with_goal,
    converters_with_goal
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_name LIKE '%Jivo%'   -- или '%поиск%', '%форм%' и т.д.
ORDER BY visit_number
```

**Интерпретация**: Jivo-цели в базе имеют lift 8–11 на шаге 1 — умеренный сигнал. Клиенты, начавшие чат, покупают в 9x чаще среднего. Если lift < 3 на всех шагах — инструмент не влияет на конверсию.

### 3. На каком шаге клиент «дозревает»

**Триггеры**: "Когда клиент готов к покупке?", "На каком визите принимается решение?"

```sql
SELECT
    visit_number,
    round(max(lift), 1)         AS max_lift,
    argMax(goal_id, lift)       AS strongest_goal_id,
    argMax(goal_name, lift)     AS strongest_goal_name,
    sum(converters_with_goal)   AS total_converters
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_id NOT IN (3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402)
GROUP BY visit_number
ORDER BY visit_number
```

**Интерпретация**: шаг с резким падением max_lift — граница «дозревания». Если на шаге 1 lift=99, а на шаге 5 уже 9 — основное решение принимается при первом визите. Рекламный контакт критически важен на ранних шагах.

### 4. Сравнение двух целей: что сильнее

**Триггеры**: "Корзина важнее поиска?", "Звонок лучше чата?", "Что стимулировать — добавление в корзину или чекаут?"

```sql
SELECT
    visit_number,
    goal_id,
    goal_name,
    round(lift, 1)            AS lift,
    clients_with_goal,
    converters_with_goal
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_id IN (21115645, 194388760)   -- Корзина vs Добавление в корзину
ORDER BY goal_id, visit_number
```

**Интерпретация**: goal_id **всегда присутствует** в результате — используй его для обратной сверки с goals_sanok.md. Цель с более высоким lift на ранних шагах — более сильный предиктор.

### 5. Динамика скоринга — здоровье системы

**Триггеры**: "Данные обновились?", "Когда был последний рефреш?", "Сколько значимых целей нашли?"

```sql
SELECT
    snapshot_date,
    count()                                             AS total_pairs,
    countIf(goal_id NOT IN (3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402)) AS signal_pairs,
    round(max(lift), 1)                                 AS max_lift,
    round(avg(lift), 1)                                 AS avg_lift
FROM ym_sanok.dm_step_goal_impact
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 7
```

### 6. Объяснение скора конкретного клиента (для dm_active_clients_scoring)

**Триггеры**: "Почему этот клиент получил высокий скор?", "Что он делал на сайте?"

```sql
-- Подставить реальный client_id
SELECT
    v.date,
    v.dateTime                              AS visit_time,
    ROW_NUMBER() OVER (
        PARTITION BY v.clientID ORDER BY v.dateTime, v.visitID
    )                                       AS visit_num,
    g AS goal_id,
    s.goal_name,
    round(s.lift, 1)                        AS lift
FROM ym_sanok.visits AS v
ARRAY JOIN v.goalsID AS g
INNER JOIN ym_sanok.dm_step_goal_impact AS s
    ON s.goal_id = g
    AND s.visit_number = toUInt8(least(
        ROW_NUMBER() OVER (PARTITION BY v.clientID ORDER BY v.dateTime, v.visitID),
        7
    ))
WHERE v.clientID = <CLIENT_ID>
  AND s.snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
ORDER BY lift DESC
```

## Уточнения и ограничения

**Новые цели серии 543662xxx (DataLayer/GTM)**: добавлены в Метрику после 28.03.2026. До этой даты данных по ним нет. Lift будет низким или нулевым до накопления достаточной выборки (≥50 клиентов на шаг). Следить через поле `clients_with_goal`.

**Цели-дубликаты "Раковины для инвалидов"**: в справочнике два разных goal_id с одинаковым URL-паттерном — 22763820 и 22763835. В `goal_name` они помечены через `(goal XXXXXXXX)` для различия. При анализе смотреть на goal_id.

**"Посетили сайт" (130300219)**: шумовая цель, lift близок к 1. Присутствует в таблице для полноты, но бесполезна для скоринга.

**ROW_NUMBER и INSERT-triggered MV**: в `dm_client_journey` (INSERT-triggered) visit_number вычислен внутри каждого батча вставки, поэтому для клиентов с несколькими визитами он неточен. Поэтому `dm_step_goal_impact` нумерует визиты напрямую из `visits` через ROW_NUMBER() — это корректно в REFRESHABLE MV, который обрабатывает все данные разом.

## Цепочка обновления

```
ym_sanok.visits (INSERT → near real-time)
         │
         ├─► dm_client_profile_mv     (INSERT-triggered, always up to date)
         │
         └─► dm_step_goal_impact_mv   REFRESH EVERY 1 DAY OFFSET 6 HOUR
                      │
                      ▼
              dm_active_clients_scoring_mv  REFRESH EVERY 1 DAY OFFSET 8 HOUR
                      (следующая витрина — строится поверх этой)
```
