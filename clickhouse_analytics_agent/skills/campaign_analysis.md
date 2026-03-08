## Skill: Анализ рекламных кампаний

### Ключевые метрики и формулы

| Метрика | Формула | Единица |
|---|---|---|
| CTR | clicks / impressions | % |
| CPC | spend / clicks | руб. |
| CPM | spend / impressions * 1000 | руб. |
| CR (конверсия) | orders / visits | % |
| CPA | spend / orders | руб. |
| ROAS | revenue / spend | × (коэффициент) |

Если нет данных о расходах в витринах — скажи прямо: "Для этого нужен Директ. Сейчас недоступен."
Показывай формулу при первом вычислении нестандартной метрики.

### КРИТИЧНО: два несмешиваемых трека в dm_campaign_funnel

В dm_campaign_funnel есть два независимых и НЕСОВМЕСТИМЫХ трека:

**Сессионный трек** (считает сессии):
`visits → pre_purchase_visits → sessions_with_purchase`

**Клиентский трек** (считает уникальных людей):
`unique_clients_pre_purchase → unique_buyers`

Делить показатели разных треков нельзя ни при каком условии.
Результат воронки >100% — это маркер смешения треков, не аномалия данных.

### Выбор таблицы

- **dm_campaigns** — суммарные метрики по кампаниям (spend, impressions, clicks, etc.)
- **dm_traffic_performance** — дневная/недельная динамика, визиты, сессии
- **dm_campaign_funnel** — воронка конверсии (ТОЛЬКО внутри одного трека)

dm_campaign_funnel НЕ содержит недельной динамики — для динамики используй dm_traffic_performance.

### Паттерн JOIN кампаний + трафика

```sql
WITH кампании AS (
    SELECT campaign_id, SUM(spend) AS spend, SUM(clicks) AS clicks
    FROM dm_campaigns
    WHERE date >= toStartOfMonth(today() - INTERVAL 1 MONTH)
      AND date < toStartOfMonth(today())
    GROUP BY campaign_id
),
трафик AS (
    SELECT campaign_id, SUM(visits) AS visits, SUM(revenue) AS revenue
    FROM dm_traffic_performance
    WHERE date >= toStartOfMonth(today() - INTERVAL 1 MONTH)
      AND date < toStartOfMonth(today())
    GROUP BY campaign_id
)
SELECT к.campaign_id,
       к.spend,
       к.clicks,
       т.visits,
       т.revenue,
       т.revenue / к.spend AS roas,
       к.spend / к.clicks AS cpc
FROM кампании к
LEFT JOIN трафик т USING (campaign_id)
WHERE к.spend > 0
ORDER BY т.revenue DESC
```

### Интерпретация результатов

- ROAS < 1 → кампания убыточна
- При ранжировании кампаний по CR или среднему чеку — всегда показывай n (количество заказов)
- Если n < 5 — помечай ⚠️, не строй выводов
- Аномалии (одна кампания = 90% выручки) — исследуй и называй в ответе

### Рекомендации

Давать рекомендации только если одновременно:
1. Данные для рекомендации есть в выгрузке
2. Канал/инструмент виден в данных
3. Для масштабирования есть CR или spend по этой сущности
