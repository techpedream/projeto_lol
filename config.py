import os


def _env(key: str, default: str) -> str:
    value = os.getenv(key)
    return value.strip() if value else default


RIOT_ID_GAME_NAME = _env("RIOT_ID_GAME_NAME", "furacaoPEDRINHO")
RIOT_ID_TAG_LINE = _env("RIOT_ID_TAG_LINE", "BR1").upper()
RIOT_PLATFORM_ROUTING = _env("RIOT_PLATFORM_ROUTING", "br1")
RIOT_REGIONAL_ROUTING = _env("RIOT_REGIONAL_ROUTING", "americas")
DATA_DIR = _env("DATA_DIR", os.path.join("data", "raw"))
CSV_DIR = _env("CSV_DIR", os.path.join("data", "csv"))
