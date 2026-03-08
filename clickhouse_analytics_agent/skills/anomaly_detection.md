## Skill: Обнаружение и расследование аномалий

### Алгоритм расследования

1. **Выгрузи исторические данные** — минимум 30–90 дней для baseline
2. **Рассчитай baseline** — mean + std за период до аномалии
3. **Флаги аномалий** — |z-score| > 2 или отклонение > 30% от среднего
4. **Сегментируй** — найди, в каком сегменте (канал, кампания, устройство) концентрируется аномалия
5. **Сформулируй гипотезу** — аномалия в данных или в бизнесе?

### Z-score в Python

```python
import numpy as np

# Рассчитай статистику baseline (исключи аномальный период):
baseline = df[df['date'] < anomaly_start]
mean_val = baseline['metric'].mean()
std_val = baseline['metric'].std()

# Флаги:
df['z_score'] = (df['metric'] - mean_val) / std_val
df['is_anomaly'] = df['z_score'].abs() > 2

result = df[df['is_anomaly']].to_markdown(index=False)
```

### Сравнение с аналогичным периодом

```sql
-- Текущая неделя vs та же неделя прошлого года:
SELECT
    toStartOfWeek(date) AS week,
    SUM(visits) AS visits_current,
    lagInFrame(SUM(visits), 52) OVER (ORDER BY toStartOfWeek(date)) AS visits_last_year,
    (SUM(visits) - lagInFrame(SUM(visits), 52) OVER (ORDER BY toStartOfWeek(date)))
    / lagInFrame(SUM(visits), 52) OVER (ORDER BY toStartOfWeek(date)) * 100 AS yoy_pct
FROM dm_traffic_performance
WHERE date >= today() - INTERVAL 1 YEAR
GROUP BY week
ORDER BY week
```

### Сегментация для локализации аномалии

```sql
-- Разбивка по каналу в аномальный день:
SELECT
    utm_medium,
    SUM(visits) AS visits,
    SUM(revenue) AS revenue
FROM dm_traffic_performance
WHERE date = '2024-03-15'  -- аномальная дата
GROUP BY utm_medium
ORDER BY visits DESC
```

### Типичные причины аномалий

| Паттерн | Вероятная причина |
|---|---|
| Резкий рост в один день | Акция, публикация, вирусный контент |
| Резкое падение в один день | Технический сбой, блокировка, изменение UTM |
| Постепенный тренд | Изменение алгоритма, сезонность, конкуренция |
| Аномалия в одном канале | Изменение ставок/бюджетов, отключение кампании |
| Аномалия в одном устройстве | Технический сбой мобильной версии/приложения |

### Вывод аномалий

```python
# Таблица с флагами:
anomalies = df[df['is_anomaly']].copy()
anomalies['отклонение'] = anomalies['z_score'].apply(
    lambda z: f"⚠️ +{z:.1f}σ" if z > 0 else f"⚠️ {z:.1f}σ"
)
result = anomalies[['date', 'metric', 'отклонение']].to_markdown(index=False)
```

Аномалия — исследуй, не игнорируй. Одна строка с 90% выручки — это сигнал, не норма.
