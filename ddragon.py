from __future__ import annotations

from typing import Any, Dict, List

import requests


def fetch_champion_dimension(locale: str = "pt_BR") -> List[Dict[str, Any]]:
    versions_response = requests.get(
        "https://ddragon.leagueoflegends.com/api/versions.json", timeout=20
    )
    versions_response.raise_for_status()
    versions = versions_response.json()
    if not versions:
        return []
    version = versions[0]
    champions_response = requests.get(
        f"https://ddragon.leagueoflegends.com/cdn/{version}/data/{locale}/champion.json",
        timeout=20,
    )
    champions_response.raise_for_status()
    payload = champions_response.json().get("data", {})

    rows: List[Dict[str, Any]] = []
    for champion in payload.values():
        tags = champion.get("tags", []) or []
        rows.append(
            {
                "championId": int(champion.get("key", 0) or 0),
                "championKey": champion.get("id", ""),
                "championName": champion.get("name", ""),
                "championTitle": champion.get("title", ""),
                "primaryClass": tags[0] if len(tags) > 0 else "",
                "secondaryClass": tags[1] if len(tags) > 1 else "",
            }
        )
    return rows
