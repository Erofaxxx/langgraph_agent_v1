"""
Segmentation tool: save_segment.

Вызывается агентом-сегментатором только после явного подтверждения маркетологом.
Сохраняет JSON-определение сегмента в SQLite через SegmentStore.
"""

import json

from langchain_core.tools import tool

from segment_store import get_segment_store


@tool
def save_segment(segment_json: str) -> str:
    """
    Save an audience segment definition to persistent storage.

    Call this ONLY after the user has explicitly confirmed ("Да" / "Yes" / "Сохрани").

    Args:
        segment_json: JSON string with the full segment object. Required fields:
            - name (str): human-readable segment name
            - description (str): what this segment represents
            - approach (str): rfm | funnel_behavioral | channel | cohort | product | multichannel
            - period (dict): {"type": "rolling|fixed|cohort|all_time", ...}
            - conditions (dict): rfm/traffic/behavior/geo_device/purchases/funnel blocks
            - primary_table (str): main ClickHouse table for materialization
            - sql_query (str): verified SELECT client_id query (NOT a COUNT query)
            - last_count (int): number of users from the trial COUNT query
            - join_tables (list): additional tables joined in the query (can be empty list)

    Returns:
        JSON with {"success": true, "segment_id": "seg_XXXXXX", "name": "...", "last_count": N}
        or {"success": false, "error": "..."} on failure.
    """
    try:
        segment = json.loads(segment_json)
    except json.JSONDecodeError as e:
        return json.dumps({"success": False, "error": f"Invalid JSON: {e}"})

    if not segment.get("name"):
        return json.dumps({"success": False, "error": "Field 'name' is required"})
    if not segment.get("sql_query"):
        return json.dumps({
            "success": False,
            "error": "Field 'sql_query' is required — run a trial COUNT query first to verify the segment",
        })

    try:
        store = get_segment_store()
        saved = store.save(segment)
        return json.dumps(
            {
                "success": True,
                "segment_id": saved["segment_id"],
                "name": saved["name"],
                "last_count": saved.get("last_count"),
            },
            ensure_ascii=False,
        )
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})
