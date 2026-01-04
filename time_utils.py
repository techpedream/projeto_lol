from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
from zoneinfo import ZoneInfo


_OUTPUT_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_unix_ms(timestamp_ms: Any) -> Dict[str, str]:
    if timestamp_ms in (None, ""):
        return {"datetime_utc": "", "datetime_brasil": ""}
    try:
        value = int(timestamp_ms)
    except (TypeError, ValueError):
        return {"datetime_utc": "", "datetime_brasil": ""}
    if value <= 0:
        return {"datetime_utc": "", "datetime_brasil": ""}
    dt_utc = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    dt_br = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    return {
        "datetime_utc": dt_utc.strftime(_OUTPUT_FORMAT),
        "datetime_brasil": dt_br.strftime(_OUTPUT_FORMAT),
    }


def add_datetime_fields(row: Dict[str, Any], base_key: str, timestamp_ms: Any) -> None:
    formatted = format_unix_ms(timestamp_ms)
    row[f"{base_key}_datetime_utc"] = formatted["datetime_utc"]
    row[f"{base_key}_datetime_brasil"] = formatted["datetime_brasil"]
