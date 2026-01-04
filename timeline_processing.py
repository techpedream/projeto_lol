from __future__ import annotations

from typing import Any, Dict, Iterable, List

from time_utils import format_unix_ms


def _get_game_phase(minute_game: int) -> str:
    # Phase buckets align with common BI splits for early/mid/late game.
    if minute_game < 14:
        return "early"
    if minute_game < 25:
        return "mid"
    return "late"


def build_timeline_frame_rows(
    match_id: str, frames: Iterable[Dict[str, Any]], match_start_ts: int | None
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for frame in frames:
        timestamp_ms = frame.get("timestamp")
        if timestamp_ms is None:
            continue
        minute_game = int(timestamp_ms // 60000)
        phase = _get_game_phase(minute_game)
        real_ts = (
            match_start_ts + timestamp_ms if match_start_ts is not None else None
        )
        real_formatted = format_unix_ms(real_ts) if real_ts else {"datetime_utc": "", "datetime_brasil": ""}
        rows.append(
            {
                "match_id": match_id,
                "timestamp": timestamp_ms,
                "minute_game": minute_game,
                "game_phase": phase,
                "realTimestamp": real_ts or "",
                "realTimestamp_datetime_utc": real_formatted["datetime_utc"],
                "realTimestamp_datetime_brasil": real_formatted["datetime_brasil"],
            }
        )
    return rows


def build_fact_match_timeline_rows(
    match_id: str,
    frames: Iterable[Dict[str, Any]],
    participants: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    participant_info: Dict[int, Dict[str, Any]] = {}
    for participant in participants:
        participant_id = participant.get("participantId")
        if participant_id is None:
            continue
        participant_info[int(participant_id)] = {
            "puuid": participant.get("puuid"),
            "summoner_name": participant.get("summonerName"),
            "championId": participant.get("championId"),
            "team_id": participant.get("teamId"),
        }

    phases = ("early", "mid", "late")
    stats: Dict[int, Dict[str, Dict[str, int]]] = {}
    for participant_id in participant_info:
        stats[participant_id] = {
            phase: {
                "kills": 0,
                "deaths": 0,
                "item_purchases": 0,
                "objectives": 0,
                "gold_total": 0,
                "xp_total": 0,
                "level_max": 0,
                "cs_total": 0,
            }
            for phase in phases
        }

    for frame in frames:
        timestamp_ms = frame.get("timestamp", 0)
        minute_game = int(timestamp_ms // 60000) if timestamp_ms else 0
        phase = _get_game_phase(minute_game)

        # Use the latest frame in each phase to represent gold/xp/cs totals.
        participant_frames = frame.get("participantFrames", {})
        for participant_id_str, payload in participant_frames.items():
            try:
                participant_id = int(participant_id_str)
            except (TypeError, ValueError):
                continue
            if participant_id not in stats:
                continue
            phase_stats = stats[participant_id][phase]
            total_gold = payload.get("totalGold")
            if total_gold is not None:
                phase_stats["gold_total"] = total_gold
            xp_total = payload.get("xp")
            if xp_total is not None:
                phase_stats["xp_total"] = xp_total
            level = payload.get("level")
            if level is not None:
                phase_stats["level_max"] = max(phase_stats["level_max"], level)
            cs_total = (payload.get("minionsKilled", 0) or 0) + (
                payload.get("jungleMinionsKilled", 0) or 0
            )
            phase_stats["cs_total"] = cs_total

        # Ignore timestamp == 0 events to avoid synthetic start markers.
        for event in frame.get("events", []):
            event_ts = event.get("timestamp", 0)
            if not event_ts:
                continue
            event_phase = _get_game_phase(int(event_ts // 60000))
            event_type = event.get("type")
            if event_type == "CHAMPION_KILL":
                killer_id = event.get("killerId")
                victim_id = event.get("victimId")
                if killer_id in stats:
                    stats[killer_id][event_phase]["kills"] += 1
                if victim_id in stats:
                    stats[victim_id][event_phase]["deaths"] += 1
                continue
            if event_type == "ITEM_PURCHASED":
                participant_id = event.get("participantId")
                if participant_id in stats:
                    stats[participant_id][event_phase]["item_purchases"] += 1
                continue
            if event_type == "ELITE_MONSTER_KILL":
                monster_type = event.get("monsterType")
                if monster_type in {"DRAGON", "BARON_NASHOR"}:
                    killer_id = event.get("killerId")
                    if killer_id in stats:
                        stats[killer_id][event_phase]["objectives"] += 1
                continue
            if event_type == "BUILDING_KILL":
                building_type = event.get("buildingType")
                if building_type == "TOWER_BUILDING":
                    killer_id = event.get("killerId")
                    if killer_id in stats:
                        stats[killer_id][event_phase]["objectives"] += 1

    rows: List[Dict[str, Any]] = []
    for participant_id, phase_stats in stats.items():
        base = participant_info.get(participant_id, {})
        for phase in phases:
            payload = phase_stats[phase]
            rows.append(
                {
                    "match_id": match_id,
                    "participant_id": participant_id,
                    "puuid": base.get("puuid"),
                    "summoner_name": base.get("summoner_name"),
                    "championId": base.get("championId"),
                    "team_id": base.get("team_id"),
                    "game_phase": phase,
                    "kills": payload["kills"],
                    "deaths": payload["deaths"],
                    "item_purchases": payload["item_purchases"],
                    "objectives": payload["objectives"],
                    "gold_total": payload["gold_total"],
                    "xp_total": payload["xp_total"],
                    "level_max": payload["level_max"],
                    "cs_total": payload["cs_total"],
                }
            )

    return rows
