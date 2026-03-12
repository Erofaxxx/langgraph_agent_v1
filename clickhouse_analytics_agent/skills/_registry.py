"""
Реестр skills — динамически подгружаемых инструкций для агента.

Каждый skill — это пара:
  router_hint : подсказка роутеру (какие ключевые слова/сценарии активируют скилл)
  full_path   : путь к .md файлу с детальными инструкциями

Добавление нового скилла:
  1. Создай skills/<name>.md
  2. Добавь запись в SKILLS ниже
  — код агента трогать не нужно.
"""

from pathlib import Path

_SKILLS_DIR = Path(__file__).parent

SKILLS: dict[str, dict] = {
    "clickhouse_querying": {
        "router_hint": (
            "SQL запрос к базе данных, выгрузить данные, написать SELECT, "
            "получить данные из ClickHouse, запрос к таблице, показать данные, "
            "сколько, топ, список, найди в базе"
        ),
        "full_path": _SKILLS_DIR / "clickhouse_querying.md",
    },
    "product_analytics": {
        "router_hint": (
          "товар|продукт|топ|sku|ассортимент|выручка по товарам|dm_products|dm_purchases|позиция|категория|штуки|количество"
        ),
        "full_path": _SKILLS_DIR / "product_analytics.md",
    },
    "python_analysis": {
        "router_hint": (
            "анализ данных Python, рассчитать метрику, посчитать, сравнить значения, "
            "обработать данные, parquet файл, pandas, DataFrame, агрегация, "
            "среднее, медиана, процент, доля, динамика"
        ),
        "full_path": _SKILLS_DIR / "python_analysis.md",
    },
    "visualization": {
        "router_hint": (
            "график, диаграмма, визуализация, нарисуй, построй график, "
            "динамика на графике, тренд, столбчатая, линейная, гистограмма, "
            "scatter, heatmap, барчарт"
        ),
        "full_path": _SKILLS_DIR / "visualization.md",
    },
    "campaign_analysis": {
        "router_hint": (
            "ROAS, CPC, CPM, CTR, CPA, кампании, расходы, бюджет, "
            "рекламные кампании, utm_campaign, dm_campaigns, конверсия кампаний, "
            "стоимость привлечения, окупаемость рекламы, spend,"
            "first touch|last touch|атрибуция|откуда покупатели|dm_orders"
        ),
        "full_path": _SKILLS_DIR / "campaign_analysis.md",
    },
    "cohort_analysis": {
        "router_hint": (
            "когорты, когортный анализ, удержание клиентов, retention, LTV, "
            "пожизненная ценность, dm_client_journey, dm_client_profile, "
            "повторные покупки, клиенты по периодам, первая покупка"
        ),
        "full_path": _SKILLS_DIR / "cohort_analysis.md",
    },
    "anomaly_detection": {
        "router_hint": (
            "аномалии, аномальные значения, резкое изменение, выбросы, "
            "почему упало, почему выросло, неожиданный скачок, странные данные, "
            "необычное поведение, резкий рост, резкое падение, исследуй причину"
        ),
        "full_path": _SKILLS_DIR / "anomaly_detection.md",
    },
    "weekly_report": {
        "router_hint": (
            "еженедельный отчёт, сводка за неделю, итоги периода, дашборд, "
            "отчёт за месяц, общая сводка, ключевые метрики за период, "
            "weekly report, WoW, week over week, итоговый отчёт"
        ),
        "full_path": _SKILLS_DIR / "weekly_report.md",
    },
    "segmentation": {
        "router_hint": (
            "сегмент аудитории, именованный сегмент, для сегмента, покажи сегмент, "
            "лояльные покупатели, тёплые лиды, аудитория из сегмента, использовать сегмент, "
            "ретаргет сегмент, атрибуция для сегмента, segment, audience, "
            "кто из сегмента, анализ по сегменту"
        ),
        "full_path": _SKILLS_DIR / "segmentation.md",
    },
}


def load_skill_instructions(active_skills: list[str]) -> str:
    """
    Загрузить и объединить инструкции для активных скиллов.

    Args:
        active_skills: список имён скиллов из SKILLS

    Returns:
        Строка с объединёнными инструкциями (или пустая строка если нет скиллов).
    """
    if not active_skills:
        return ""

    parts: list[str] = []
    for skill_name in active_skills:
        skill = SKILLS.get(skill_name)
        if skill is None:
            continue
        path: Path = skill["full_path"]
        try:
            content = path.read_text(encoding="utf-8").strip()
            if content:
                parts.append(content)
        except Exception as exc:
            # Скилл не загружен — агент продолжит без него
            print(f"⚠️  Could not load skill '{skill_name}' from {path}: {exc}")

    return "\n\n---\n\n".join(parts)
