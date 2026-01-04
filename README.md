# Riot BI starter

Minimal pipeline to pull data from the Riot API for a single Riot ID and store raw JSON
files for BI exploration.

## Setup

1. Create `.env` using `.env.example` as a template and set `RIOT_API_KEY`.
2. Install deps: `pip install -r requirements.txt`
3. Run: `python api.py`

The output files land in `data/raw/` and CSVs in `data/csv/`.
