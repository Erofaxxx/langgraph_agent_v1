"""
Named ClickHouse queries for GET /api/tables/{query_name}.

Структура каждого запроса:
  description      — короткое описание для фронта
  sql              — SELECT без ORDER BY и LIMIT (добавляются динамически)
  sortable_columns — белый список колонок, по которым разрешена сортировка

Добавляй новые запросы сюда — endpoint подхватит их автоматически.
"""

QUERIES: dict[str, dict] = {
    "bad_placements": {
        "description": "Плохие площадки",
        "sql": """
            SELECT
                `Placement`,
                `CampaignName`,
                cpc,
                cost,
                purchase_revenue,
                roas,
                goal_score_rate,
                tier12_conversions,
                med_cpc_campaign,
                zone_status
            FROM ym_sanok.bad_placements_v3
            WHERE (zone_status != 'pending' OR zone_status IS NULL)
        """,
        "sortable_columns": ["Placement", "CampaignName", "cpc", "cost", "purchase_revenue", "roas", "goal_score_rate", "tier12_conversions", "med_cpc_campaign", "zone_status"],
        "filterable_zone_status": True,
    },
    "bad_keywords": {
        "description": "Плохие ключевые запросы",
        "sql": """
            SELECT
                `Criterion`,
                `CampaignName`,
                `AdGroupName`,
                cpc,
                goal_score_rate,
                avg_bid,
                cpc_to_bid_ratio,
                purchase_revenue,
                roas,
                med_roas,
                tier12_conversions,
                med_goal_score_rate,
                zone_status
            FROM ym_sanok.bad_keywords_v1
            WHERE (zone_status != 'pending')
        """,
        "sortable_columns": ["Criterion", "CampaignName", "AdGroupName", "cpc", "goal_score_rate", "avg_bid", "cpc_to_bid_ratio", "purchase_revenue", "roas", "med_roas", "tier12_conversions", "med_goal_score_rate", "zone_status"],
        "filterable_zone_status": True,
    },
}
