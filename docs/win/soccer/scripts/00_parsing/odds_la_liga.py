#!/usr/bin/env python3
# docs/win/soccer/scripts/00_parsing/odds_la_liga.py

import csv
import json
import os
import time
import traceback
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

API_KEY_ENV = "API_ODDS"
BASE_URL = "https://api.odds-api.io/v3"
ET = ZoneInfo("America/New_York")

LEAGUE_NAME = "LA_LIGA"
LEAGUE_SLUG = "spain-laliga"
BOOKMAKER = "DraftKings"
OUT_SLUG = "la_liga"

FIELDS = [
    "sport",
    "league",
    "game_id",
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "dk_home_decimal",
    "dk_draw_decimal",
    "dk_away_decimal",
    "dk_over25_decimal",
    "dk_under25_decimal",
    "dk_over35_decimal",
    "dk_under35_decimal",
    "btts_yes",
    "btts_no",
]

SPORTSBOOK_OUT_DIR = Path(f"docs/win/soccer/00_intake/sportsbook/{OUT_SLUG}")
RAW_OUT_DIR = Path(f"docs/win/soccer/00_intake/odds_api_raw/{OUT_SLUG}")
LOG_DIR = Path("docs/win/soccer/errors/00_intake")

SPORTSBOOK_OUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / f"odds_{OUT_SLUG}.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== odds_{OUT_SLUG} RUN {datetime.now(ET).isoformat()} ===\n")


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(ET).isoformat()} | {msg}\n")


def get_api_key():
    key = os.environ.get(API_KEY_ENV, "").strip()
    if not key:
        raise RuntimeError(f"{API_KEY_ENV} environment variable is not set")
    return key


def request_json(path, params):
    url = f"{BASE_URL}{path}"
    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code} for {url} | body={response.text[:500]}"
        )

    return response.json()


def to_match_date_time(date_str):
    if not date_str:
        return "", ""

    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        dt_et = dt.astimezone(ET)
        return dt_et.strftime("%Y_%m_%d"), dt_et.strftime("%I:%M %p")
    except Exception:
        return "", ""


def get_bookmaker_markets(odds_payload):
    books = odds_payload.get("bookmakers", {})
    if not isinstance(books, dict):
        return []

    markets = books.get(BOOKMAKER, [])
    if isinstance(markets, list):
        return markets

    return []


def find_market(markets, wanted_name):
    wanted = wanted_name.strip().lower()

    for market in markets:
        if str(market.get("name", "")).strip().lower() == wanted:
            return market

    return None


def parse_ml(markets):
    market = find_market(markets, "ML")
    if not market:
        return "", "", ""

    odds = market.get("odds", [])
    if not isinstance(odds, list) or not odds:
        return "", "", ""

    row = odds[0]
    if not isinstance(row, dict):
        return "", "", ""

    return (
        str(row.get("home", "") or ""),
        str(row.get("draw", "") or ""),
        str(row.get("away", "") or ""),
    )


def odds_line_value(row):
    for key in ("line", "hdp", "max", "total", "points", "point"):
        val = row.get(key)
        if val not in ("", None):
            return str(val)
    return ""


def parse_total_line(markets, target_line):
    market = find_market(markets, "Totals")
    if not market:
        return "", ""

    odds = market.get("odds", [])
    if not isinstance(odds, list):
        return "", ""

    for row in odds:
        if not isinstance(row, dict):
            continue

        over = str(row.get("over", "") or row.get("Over", "") or "")
        under = str(row.get("under", "") or row.get("Under", "") or "")
        line_raw = odds_line_value(row)

        if line_raw:
            try:
                if abs(float(line_raw) - float(target_line)) < 0.001:
                    return over, under
            except Exception:
                pass

        label = " ".join(str(v) for v in row.values()).lower()
        if str(target_line) in label:
            return over, under

    return "", ""


def parse_btts(markets):
    market = find_market(markets, "Both Teams To Score")
    if not market:
        return "", ""

    odds = market.get("odds", [])
    if not isinstance(odds, list):
        return "", ""

    for row in odds:
        if not isinstance(row, dict):
            continue

        yes = row.get("yes") or row.get("Yes") or row.get("YES") or ""
        no = row.get("no") or row.get("No") or row.get("NO") or ""

        if yes or no:
            return str(yes or ""), str(no or "")

    return "", ""


def fetch_events(api_key):
    payload = request_json(
        "/events",
        {
            "apiKey": api_key,
            "sport": "football",
            "league": LEAGUE_SLUG,
            "status": "pending",
            "bookmaker": BOOKMAKER,
            "limit": 100,
        },
    )

    if isinstance(payload, list):
        return payload

    log(f"WARNING: events response was not list: {payload}")
    return []


def fetch_odds(api_key, event_id):
    payload = request_json(
        "/odds",
        {
            "apiKey": api_key,
            "eventId": event_id,
            "bookmakers": BOOKMAKER,
        },
    )

    if isinstance(payload, dict):
        return payload

    return {}


def build_row(event, odds_payload):
    markets = get_bookmaker_markets(odds_payload)

    dk_home, dk_draw, dk_away = parse_ml(markets)
    over25, under25 = parse_total_line(markets, 2.5)
    over35, under35 = parse_total_line(markets, 3.5)
    btts_yes, btts_no = parse_btts(markets)

    match_date, match_time = to_match_date_time(
        odds_payload.get("date") or event.get("date") or ""
    )

    return {
        "sport": "soccer",
        "league": LEAGUE_NAME,
        "game_id": str(odds_payload.get("id") or event.get("id") or ""),
        "match_date": match_date,
        "match_time": match_time,
        "home_team": odds_payload.get("home") or event.get("home") or "",
        "away_team": odds_payload.get("away") or event.get("away") or "",
        "dk_home_decimal": dk_home,
        "dk_draw_decimal": dk_draw,
        "dk_away_decimal": dk_away,
        "dk_over25_decimal": over25,
        "dk_under25_decimal": under25,
        "dk_over35_decimal": over35,
        "dk_under35_decimal": under35,
        "btts_yes": btts_yes,
        "btts_no": btts_no,
    }


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def main():
    api_key = get_api_key()
    today = datetime.now(ET).strftime("%Y_%m_%d")

    csv_out_path = SPORTSBOOK_OUT_DIR / f"{today}_{OUT_SLUG}_soccer.csv"
    json_out_path = RAW_OUT_DIR / f"{today}_{OUT_SLUG}_soccer_odds.json"

    rows = []
    raw_records = []
    odds_errors = 0

    log(f"START league={LEAGUE_NAME} slug={LEAGUE_SLUG} bookmaker={BOOKMAKER}")

    events = fetch_events(api_key)
    log(f"EVENTS league={LEAGUE_NAME} count={len(events)}")

    for event in events:
        event_id = str(event.get("id", "")).strip()
        if not event_id:
            log("SKIP missing event id")
            continue

        try:
            odds_payload = fetch_odds(api_key, event_id)
            row = build_row(event, odds_payload)
            rows.append(row)
            raw_records.append({"event": event, "odds": odds_payload, "row": row})

            log(
                f"ROW league={LEAGUE_NAME} game_id={row['game_id']} "
                f"{row['away_team']} at {row['home_team']} "
                f"ML=({row['dk_home_decimal']},{row['dk_draw_decimal']},{row['dk_away_decimal']}) "
                f"O25/U25=({row['dk_over25_decimal']},{row['dk_under25_decimal']}) "
                f"O35/U35=({row['dk_over35_decimal']},{row['dk_under35_decimal']}) "
                f"BTTS=({row['btts_yes']},{row['btts_no']})"
            )

        except Exception as e:
            odds_errors += 1
            log(f"ERROR odds event_id={event_id}: {e}")
            log(traceback.format_exc())

        time.sleep(0.2)

    write_csv(csv_out_path, rows)

    with open(json_out_path, "w", encoding="utf-8") as f:
        json.dump(raw_records, f, indent=2)

    log("--- SUMMARY ---")
    log(f"Rows written: {len(rows)}")
    log(f"Odds errors: {odds_errors}")
    log(f"CSV output: {csv_out_path}")
    log(f"JSON output: {json_out_path}")

    if events and not rows:
        log("STATUS: FAILED")
        raise RuntimeError("Events were found but no rows were written")

    log("STATUS: SUCCESS")
    print(f"WROTE {csv_out_path} rows={len(rows)}")
    print(f"WROTE {json_out_path} rows={len(raw_records)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL ERROR: {e}")
        log(traceback.format_exc())
        log("STATUS: FAILED")
        raise
