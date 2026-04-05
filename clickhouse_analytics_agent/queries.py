"""
Named ClickHouse queries for GET /api/tables/{query_name}.

Структура каждого запроса:
  description      — короткое описание для фронта
  sql              — SELECT без ORDER BY и LIMIT (добавляются динамически)
  sortable_columns — белый список колонок, по которым разрешена сортировка

Добавляй новые запросы сюда — endpoint подхватит их автоматически.
"""

QUERIES: dict[str, dict] = {
    "top_campaigns": {
        "description": "Топ кампании по расходу",
        "sql": """
            SELECT
                campaign_id,
                campaign_name,
                sum(clicks)      AS clicks,
                sum(impressions) AS impressions,
                sum(spend)       AS spend,
                round(sum(spend) / nullIf(sum(clicks), 0), 4) AS cpc
            FROM campaigns
            GROUP BY campaign_id, campaign_name
        """,
        "sortable_columns": ["clicks", "impressions", "spend", "cpc", "campaign_name"],
    },
    "daily_stats": {
        "description": "Статистика по дням (последние 30 дней)",
        "sql": """
            SELECT
                toDate(event_time) AS date,
                sum(clicks)        AS clicks,
                sum(impressions)   AS impressions,
                sum(spend)         AS spend
            FROM events
            WHERE event_time >= today() - 30
            GROUP BY date
        """,
        "sortable_columns": ["date", "clicks", "impressions", "spend"],
    },
    "bad_placements": {
        "description": "Плохие площадки (zone_status = red)",
        "sql": """
            SELECT
                Placement,
                CampaignName,
                cost,
                cpc,
                purchase_revenue AS purchaseRevenue,
                roas
            FROM ym_sanok.bad_placements_v2
            WHERE zone_status = 'red'
        """,
        "sortable_columns": ["Placement", "CampaignName", "cost", "cpc", "purchaseRevenue", "roas"],
    },
}
