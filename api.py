import csv
import os

from dotenv import load_dotenv

from config import (
    CSV_DIR,
    DATA_DIR,
    RIOT_ID_GAME_NAME,
    RIOT_ID_TAG_LINE,
    RIOT_PLATFORM_ROUTING,
    RIOT_REGIONAL_ROUTING,
)
from csv_exporter import flatten_dict, write_csv, write_data_as_csv
from ddragon import fetch_champion_dimension
from riot import (
    RiotClient,
    get_account_by_riot_id,
    get_champion_mastery_by_puuid,
    get_league_entries_by_summoner_id,
    get_match,
    get_match_ids_by_puuid,
    get_match_timeline,
    get_summoner_by_puuid,
)
from storage import write_json
from time_utils import add_datetime_fields, format_unix_ms
from timeline_processing import (
    build_fact_match_timeline_rows,
    build_timeline_frame_rows,
)


def _upsert_fact_match_player(
    path: str, rows: list[dict], allowed_queues: set[int] | None = None
) -> None:
    if not rows:
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    existing_rows: list[dict] = []
    existing_keys: set[tuple[str, str]] = set()
    if os.path.exists(path):
        with open(path, "r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                key = (row.get("match_id", ""), row.get("puuid", ""))
                existing_keys.add(key)
            if allowed_queues is None:
                existing_rows.append(row)
            else:
                queue_id = row.get("queue_id")
                try:
                    queue_id_int = int(queue_id) if queue_id not in (None, "") else None
                except (TypeError, ValueError):
                    queue_id_int = None
                if queue_id_int in allowed_queues:
                    existing_rows.append(row)

    for row in rows:
        if allowed_queues is not None:
            queue_id = row.get("queue_id")
            if queue_id not in allowed_queues:
                continue
        key = (str(row.get("match_id", "")), str(row.get("puuid", "")))
        if key in existing_keys:
            continue
        existing_keys.add(key)
        existing_rows.append(row)

    fieldnames = sorted({key for row in existing_rows for key in row.keys()})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_rows:
            writer.writerow(row)


def _get_ranked_match_ids(
    client: RiotClient,
    puuid: str,
    queues: list[int],
    desired: int,
    batch: int = 20,
) -> list[str]:
    match_ids: list[str] = []
    seen: set[str] = set()
    start = 0
    while len(match_ids) < desired:
        before_len = len(match_ids)
        for queue_id in queues:
            ids = get_match_ids_by_puuid(
                client, puuid, count=batch, start=start, queue=queue_id
            )
            for match_id in ids:
                if match_id in seen:
                    continue
                seen.add(match_id)
                match_ids.append(match_id)
        if len(match_ids) == before_len:
            break
        start += batch
    return match_ids


def main() -> None:
    load_dotenv("senhas.env")
    api_key = os.getenv("RIOT_API_KEY", "").strip()
    client = RiotClient(
        api_key=api_key,
        platform_routing=RIOT_PLATFORM_ROUTING,
        regional_routing=RIOT_REGIONAL_ROUTING,
    )

    account = get_account_by_riot_id(client, RIOT_ID_GAME_NAME, RIOT_ID_TAG_LINE)
    puuid = account["puuid"]
    summoner = get_summoner_by_puuid(client, puuid)
    ranked_queues = [420, 440]
    desired_match_count = 20
    match_ids = _get_ranked_match_ids(
        client, puuid, ranked_queues, desired=desired_match_count
    )
    summoner_id = summoner.get("id")
    league_entries = []
    if summoner_id:
        league_entries = get_league_entries_by_summoner_id(client, summoner_id)
    else:
        print("Warning: summoner 'id' missing; skipping league entries.")
    champion_mastery = get_champion_mastery_by_puuid(client, puuid)
    add_datetime_fields(summoner, "revisionDate", summoner.get("revisionDate"))
    for mastery in champion_mastery:
        add_datetime_fields(mastery, "lastPlayTime", mastery.get("lastPlayTime"))

    write_json(os.path.join(DATA_DIR, "account.json"), account)
    write_json(os.path.join(DATA_DIR, "summoner.json"), summoner)
    write_json(os.path.join(DATA_DIR, "league_entries.json"), league_entries)
    write_json(os.path.join(DATA_DIR, "champion_mastery.json"), champion_mastery)

    write_data_as_csv(os.path.join(CSV_DIR, "account.csv"), account)
    write_data_as_csv(os.path.join(CSV_DIR, "summoner.csv"), summoner)
    write_data_as_csv(os.path.join(CSV_DIR, "league_entries.csv"), league_entries)
    write_data_as_csv(os.path.join(CSV_DIR, "champion_mastery.csv"), champion_mastery)
    champion_dimension = fetch_champion_dimension()
    write_csv(os.path.join(CSV_DIR, "dim_champion.csv"), champion_dimension)

    match_rows = []
    timeline_rows = []
    fact_match_player_rows = []
    fact_match_timeline_rows = []
    allowed_queues = {420, 440}
    ranked_matches: list[tuple[str, dict, dict]] = []
    for match_id in match_ids:
        match = get_match(client, match_id)
        info = match.get("info", {})
        if info.get("queueId") not in allowed_queues:
            continue
        ranked_matches.append((match_id, match, info))

    ranked_matches.sort(
        key=lambda item: item[2].get("gameStartTimestamp") or 0, reverse=True
    )
    ranked_matches = ranked_matches[:desired_match_count]
    match_ids = [match_id for match_id, _, _ in ranked_matches]

    write_json(os.path.join(DATA_DIR, "match_ids.json"), match_ids)
    write_data_as_csv(os.path.join(CSV_DIR, "match_ids.csv"), match_ids)

    for match_id, match, info in ranked_matches:
        timeline = get_match_timeline(client, match_id)
        write_json(os.path.join(DATA_DIR, "matches", f"{match_id}.json"), match)
        write_json(
            os.path.join(DATA_DIR, "match_timelines", f"{match_id}.json"), timeline
        )

        match_start_ts = info.get("gameStartTimestamp")
        match_row = flatten_dict(match)
        match_row["matchId"] = match_id
        add_datetime_fields(match_row, "info.gameCreation", info.get("gameCreation"))
        add_datetime_fields(
            match_row, "info.gameStartTimestamp", info.get("gameStartTimestamp")
        )
        match_rows.append(match_row)

        frames = timeline.get("info", {}).get("frames", [])
        timeline_rows.extend(build_timeline_frame_rows(match_id, frames, match_start_ts))
        fact_match_timeline_rows.extend(
            build_fact_match_timeline_rows(match_id, frames, info.get("participants", []))
        )

        game_time = format_unix_ms(match_start_ts)
        for participant in info.get("participants", []):
            cs_total = participant.get("totalMinionsKilled", 0) + participant.get(
                "neutralMinionsKilled", 0
            )
            game_version = info.get("gameVersion", "")
            fact_match_player_rows.append(
                {
                    "match_id": match_id,
                    "puuid": participant.get("puuid"),
                    "summoner_name": participant.get("summonerName"),
                    "game_datetime": game_time["datetime_utc"],
                    "game_datetime_utc": game_time["datetime_utc"],
                    "game_datetime_brasil": game_time["datetime_brasil"],
                    "patch": game_version[:5] if game_version else "",
                    "queue_id": info.get("queueId"),
                    "game_duration": info.get("gameDuration"),
                    "win": 1 if participant.get("win") else 0,
                    "championId": participant.get("championId"),
                    "champion": participant.get("championName"),
                    "lane": participant.get("lane"),
                    "role": participant.get("teamPosition"),
                    "kills": participant.get("kills"),
                    "deaths": participant.get("deaths"),
                    "assists": participant.get("assists"),
                    "cs": cs_total,
                    "gold": participant.get("goldEarned"),
                    "vision_score": participant.get("visionScore"),
                    "damage": participant.get("totalDamageDealtToChampions"),
                }
            )

    write_csv(os.path.join(CSV_DIR, "matches.csv"), match_rows)
    write_csv(os.path.join(CSV_DIR, "match_timelines.csv"), timeline_rows)
    _upsert_fact_match_player(
        os.path.join(CSV_DIR, "fact_match_player.csv"),
        fact_match_player_rows,
        allowed_queues=allowed_queues,
    )
    write_csv(
        os.path.join(CSV_DIR, "fact_match_timeline_clean.csv"),
        fact_match_timeline_rows,
    )

    print(f"Saved {len(match_ids)} matches for {RIOT_ID_GAME_NAME}#{RIOT_ID_TAG_LINE}.")


if __name__ == "__main__":
    main()
