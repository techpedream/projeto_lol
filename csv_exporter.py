import csv
import json
import os
from typing import Any, Dict, Iterable, List


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def flatten_dict(data: Dict[str, Any], prefix: str = "", sep: str = ".") -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}{sep}{key}" if prefix else key
        if isinstance(value, dict):
            flattened.update(flatten_dict(value, prefix=full_key, sep=sep))
        elif isinstance(value, list):
            flattened[full_key] = json.dumps(value, ensure_ascii=True)
        else:
            flattened[full_key] = value
    return flattened


def _write_rows(path: str, rows: List[Dict[str, Any]]) -> None:
    directory = os.path.dirname(path)
    if directory:
        _ensure_dir(directory)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows_list = list(rows)
    if not rows_list:
        _write_rows(path, [])
        return
    _write_rows(path, rows_list)


def write_data_as_csv(path: str, data: Any) -> None:
    if isinstance(data, dict):
        _write_rows(path, [flatten_dict(data)])
        return

    if isinstance(data, list):
        if data and all(isinstance(item, dict) for item in data):
            rows = [flatten_dict(item) for item in data]
            _write_rows(path, rows)
        else:
            rows = [{"value": item} for item in data]
            _write_rows(path, rows)
        return

    _write_rows(path, [{"value": data}])
