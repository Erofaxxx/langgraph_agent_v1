# Lift-анализ целей по шагам визитов

## Таблица ym_sanok.dm_step_goal_impact

Фундамент скоринга. Для каждой пары (номер визита × цель Метрики) вычисляет, насколько выполнение цели повышает вероятность покупки. Шаги 1-7 (медиана покупателя = 3 визита), минимум 50 клиентов на комбинацию.

**Поля**: visit_number (1-7), goal_id, goal_name, clients_at_step, clients_with_goal, clients_without_goal, converters_with_goal, converters_without_goal, rate_with_goal, rate_without_goal, lift, snapshot_date.

## Отличие от magnetto

| Параметр | Magnetto | Sanok |
|---|---|---|
| Конверсия | CRM-сделка (has_crm_created=1) | Покупка (has_purchased=1) |
| Базовая конверсия | ~0.09% | ~1.19% |
| Шаги | 1–10 | **1–7** |
| Мин. выборка | 20 клиентов | **50 клиентов** |

## Как работает lift

```
rate_with_goal    = converters_with_goal / clients_with_goal
rate_without_goal = converters_without_goal / clients_without_goal
lift              = rate_with_goal / rate_without_goal
```

**Lift = 96**: клиент, выполнивший цель, конвертируется в 96 раз чаще. Базовая конверсия ~1.19%.

## Интерпретация lift

| Диапазон | Значение | Пример |
|----------|----------|--------|
| > 50 | Почти гарантия | Оформление заказа (96) — сильнейший сигнал |
| 20-50 | Сильный сигнал | Корзина (~56), Заполнил контакты |
| 5-20 | Умеренный | Просмотр товара (~12), Поиск по сайту (~33) |
| 1-5 | Слабый | Переход в соцсеть |
| < 1 | Негативная корреляция | Ассоциация с НЕпокупкой |

**Транзакционные** (исключать из рекомендаций): Ecommerce: покупка (3000178943), Заказ оформлен (31297300), серия 543662xxx (393, 395, 396, 401, 402) — срабатывают ПОСЛЕ покупки.

## Реально полезные цели (для рекомендаций)

**Шаг 1 (первый визит):**
- Оформление заказа → lift ~96
- Корзина → lift ~56
- Автоцель: поиск по сайту → lift ~33
- Добавление в корзину → lift ~28
- Заполнил контактные данные → lift ~25

**Шаги 2-5:** те же цели, lift может расти или снижаться в зависимости от поведения.
**Шаги 6-7:** lift большинства целей снижается. Самые значимые: Корзина, Оформление заказа.

## SQL-шаблоны

### Какие цели стимулировать в рекламе
```sql
SELECT goal_name, visit_number, lift, clients_with_goal, converters_with_goal
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND lift > 5
  AND goal_id NOT IN (3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402)
ORDER BY lift DESC
```

### Работает ли конкретный инструмент (Jivo, поиск, корзина)
```sql
SELECT visit_number, goal_name, lift, clients_with_goal, converters_with_goal
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_name LIKE '%корзин%'  -- или '%Jivo%', '%поиск%'
ORDER BY visit_number
```

### На каком шаге клиент "дозревает"
```sql
SELECT visit_number,
       max(lift) AS max_lift,
       argMax(goal_name, lift) AS strongest_goal,
       sum(converters_with_goal) AS total_converters
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_id NOT IN (3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402)
GROUP BY visit_number
ORDER BY visit_number
```

### Сравнение двух целей
```sql
SELECT visit_number, goal_name, lift, clients_with_goal
FROM ym_sanok.dm_step_goal_impact
WHERE snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
  AND goal_name IN ('Корзина', 'Уникальный звонок')
ORDER BY goal_name, visit_number
```
