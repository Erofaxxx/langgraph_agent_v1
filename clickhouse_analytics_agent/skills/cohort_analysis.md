## Skill: Когортный анализ

### Ключевые таблицы

- **dm_client_journey** — события клиентского пути (только clientID > 0)
- **dm_client_profile** — профиль клиента, дата первой покупки (только clientID > 0)
- **dm_ml_features** — ML-признаки клиентов (только clientID > 0)

Важно: dm_traffic_performance считает ВСЕ визиты включая анонимные (clientID = 0).
Разница с клиентскими таблицами = анонимные сессии. Это норма, не ошибка.

### Когортирование по первой покупке

```sql
-- Когорты по месяцу первой покупки:
WITH первые_покупки AS (
    SELECT clientID,
           toStartOfMonth(MIN(order_date)) AS cohort_month
    FROM dm_client_journey
    WHERE order_date IS NOT NULL
    GROUP BY clientID
),
активность AS (
    SELECT j.clientID,
           п.cohort_month,
           toStartOfMonth(j.order_date) AS activity_month,
           SUM(j.revenue) AS revenue
    FROM dm_client_journey j
    JOIN первые_покупки п USING (clientID)
    GROUP BY j.clientID, п.cohort_month, activity_month
)
SELECT cohort_month,
       activity_month,
       COUNT(DISTINCT clientID) AS active_clients,
       SUM(revenue) AS total_revenue
FROM активность
GROUP BY cohort_month, activity_month
ORDER BY cohort_month, activity_month
```

### Retention rate

```python
# Retention = клиенты вернувшиеся в месяц T+N / размер когорты
pivot = df.pivot_table(
    index='cohort_month',
    columns='activity_month',
    values='active_clients',
    aggfunc='sum'
)
# Первый столбец = размер когорты (месяц 0)
cohort_sizes = pivot.iloc[:, 0]
retention = pivot.divide(cohort_sizes, axis=0) * 100
```

### LTV (Lifetime Value)

```python
# LTV = суммарная выручка когорты нарастающим итогом
ltv = df.sort_values('activity_month')
ltv['cumulative_revenue'] = ltv.groupby('cohort_month')['total_revenue'].cumsum()
ltv['ltv_per_client'] = ltv['cumulative_revenue'] / ltv['cohort_size']
```

### Паттерн анализа повторных покупок

```sql
SELECT clientID,
       COUNT(DISTINCT order_date) AS order_count,
       MIN(order_date) AS first_order,
       MAX(order_date) AS last_order,
       SUM(revenue) AS total_revenue,
       dateDiff('day', MIN(order_date), MAX(order_date)) AS days_active
FROM dm_client_journey
WHERE order_date >= '2024-01-01'
GROUP BY clientID
HAVING order_count >= 2
ORDER BY total_revenue DESC
LIMIT 10000
```

### Интерпретация

- Retention M1 < 20% — норма для большинства e-commerce
- Retention M1 > 40% — высокий показатель, исследуй сегмент
- При анализе LTV по когортам — сравнивай когорты одинаковой зрелости (одинаковое число месяцев наблюдения)
