"""
ScoringIntelligenceAgent — подагент скоринга клиентов для Sanok.

Специализируется на таблицах:
  - dm_active_clients_scoring — скоринг клиентов (hot/warm/cold, price_tier, рекомендации)
  - dm_step_goal_impact      — lift-анализ целей по шагам визитов

Архитектура зеркалит главного агента: StateGraph agent⟷tools,
те же инструменты (clickhouse_query, python_analysis, list_tables),
4-слойная компрессия, prompt caching.
"""

from pathlib import Path
from typing import Optional

from config import MODEL
from subagents.base import BaseSubAgent

_SKILLS_DIR = Path(__file__).resolve().parent.parent / "skills"

_SKILL_FILES = [
    _SKILLS_DIR / "scoring_clients.md",
    _SKILLS_DIR / "scoring_step_impact.md",
]

_SYSTEM_PROMPT = """Ты — аналитик скоринга клиентов и конверсионных путей. Работаешь с ClickHouse-базой интернет-магазина сантехники Sanok (цикл покупки: медиана 7 дней, p90 = 63 дня, конверсия ~1.19%).

Твоя задача — анализировать:
- Скоринг клиентов: кто горячий, кого ретаргетить, когда и чем
- Ценовой тир: кто смотрит дорогие товары (high ≥25k руб) — им приоритет выше
- Lift-анализ целей: какие действия на каком шаге увеличивают конверсию в покупку
- Ecommerce-сигналы: has_cart, has_checkout — сильнейшие индикаторы

## Схема базы данных

{schema_section}

## Принципы работы

- Конверсия в Sanok = покупка (has_purchased=1), не CRM-сделка
- Шаги визитов 1-7 (не 1-10 как в magnetto) — медиана покупателя = 3 визита
- Для dm_active_clients_scoring фильтруй `snapshot_date = (SELECT max(snapshot_date) ...)`
- Для dm_step_goal_impact исключай транзакционные цели из рекомендаций: 3000178943, 31297300, 543662393, 543662395, 543662396, 543662401, 543662402
- Ценовой рычаг: клиент с дорогим товаром получает boost приоритета (high-price → WARM даже при lift_score=25)
- price_tier = unknown для ~66% клиентов (данные только по проданным товарам)
- Числа с разделителями тысяч: 1 234 567
- Язык — русский, Markdown

## Доменные инструкции

{skill_section}"""


class ScoringIntelligenceAgent(BaseSubAgent):
    """Sub-agent for client scoring analytics (Sanok)."""

    _SCHEMA_TABLES = ["dm_active_clients_scoring", "dm_step_goal_impact"]

    def __init__(self, model: str = MODEL) -> None:
        skill_text = self._load_skill_files(_SKILL_FILES)
        prompt = _SYSTEM_PROMPT.replace("{skill_section}", skill_text)
        super().__init__(system_prompt=prompt, max_iterations=10, model=model, schema_tables=self._SCHEMA_TABLES)


# ─── Singleton cache ──────────────────────────────────────────────────────────
_agents: dict[str, ScoringIntelligenceAgent] = {}


def get_scoring_agent(model: Optional[str] = None) -> ScoringIntelligenceAgent:
    """Return (or create) a cached ScoringIntelligenceAgent instance."""
    key = model or MODEL
    if key not in _agents:
        _agents[key] = ScoringIntelligenceAgent(model=key)
    return _agents[key]
