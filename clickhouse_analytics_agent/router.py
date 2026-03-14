"""
Query router — классифицирует запрос пользователя и возвращает список активных skills.

Использует дешёвую модель (Haiku) для быстрой классификации (~$0.0001 за вызов).
При любой ошибке возвращает [] — агент продолжает работу без skills (graceful fallback).

Пример:
    from router import classify
    active = classify("Какой ROAS у кампаний за прошлый месяц?")
    # → ["clickhouse_querying", "campaign_analysis"]
"""

import json
import re
from typing import Optional

from langchain_openai import ChatOpenAI

from config import OPENROUTER_API_KEY, ROUTER_MODEL
from skills._registry import SKILLS

# ─── Ленивый синглтон роутера ─────────────────────────────────────────────────
_router_llm: Optional[ChatOpenAI] = None


def _get_router_llm() -> ChatOpenAI:
    """Создать (один раз) ChatOpenAI клиент для роутера."""
    global _router_llm
    if _router_llm is None:
        _router_llm = ChatOpenAI(
            model=ROUTER_MODEL,
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            max_tokens=512,
            temperature=0,
            default_headers={
                "HTTP-Referer": "https://server.asktab.ru",
                "X-Title": "ClickHouse Analytics Agent Router",
            },
        )
    return _router_llm


def _build_router_prompt() -> str:
    """
    Автосгенерировать системный промпт роутера из реестра skills.
    При добавлении нового скилла в _registry.py — промпт обновляется автоматически.
    """
    skill_list = "\n".join(
        f'- "{name}": {info["router_hint"]}'
        for name, info in SKILLS.items()
    )
    return f"""Ты — классификатор запросов аналитического агента. Твоя задача — определить, какие skills нужны для ответа на запрос пользователя.

Доступные skills и когда их активировать:
{skill_list}

Правила:
- Верни JSON-массив с именами нужных skills: ["skill1", "skill2"]
- Если запрос требует данных из ClickHouse → обязательно включи "clickhouse_querying"
- Если запрос требует вычислений/анализа → обязательно включи "python_analysis"
- Если запрос про график/визуализацию → включи "visualization"
- Для аналитических вопросов без явного графика включи и "clickhouse_querying" и "python_analysis"
- Если запрос не требует данных (приветствие, общий вопрос) → верни []
- Не добавляй skills которые явно не нужны
- Отвечай ТОЛЬКО валидным JSON-массивом без объяснений

Примеры:
- "Сколько визитов за прошлый месяц?" → ["clickhouse_querying"]
- "Какой ROAS у кампаний? Построй график" → ["clickhouse_querying", "python_analysis", "visualization", "campaign_analysis"]
- "Когорты клиентов за 2024 год" → ["clickhouse_querying", "python_analysis", "cohort_analysis"]
- "Привет" → []
"""


def classify(query: str) -> list[str]:
    """
    Классифицировать запрос и вернуть список активных skills.

    Args:
        query: Текст запроса пользователя.

    Returns:
        Список имён skills из SKILLS. При ошибке — пустой список.
    """
    if not query or not query.strip():
        return []

    try:
        llm = _get_router_llm()
        system_prompt = _build_router_prompt()

        response = llm.invoke([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query},
        ])

        raw = response.content if isinstance(response.content, str) else str(response.content)
        raw = raw.strip()

        # Снять возможную markdown-обёртку: ```json [...] ```
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
        if match:
            raw = match.group(1).strip()

        # Найти первый JSON-массив в ответе
        arr_match = re.search(r"\[.*?\]", raw, re.DOTALL)
        if arr_match:
            raw = arr_match.group(0)

        parsed = json.loads(raw)

        if not isinstance(parsed, list):
            return []

        # Оставить только известные skills
        valid = [s for s in parsed if isinstance(s, str) and s in SKILLS]

        if valid:
            print(f"🎯 Router activated skills: {valid}")
        else:
            print("🎯 Router: no skills needed")

        return valid

    except Exception as exc:
        # Graceful fallback — агент работает без skills
        print(f"⚠️  Router error (using no skills): {exc}")
        return []
