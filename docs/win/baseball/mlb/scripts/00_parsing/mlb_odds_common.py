from __future__ import annotations

import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests


ESPN_BASE = "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb"
DRAFTKINGS_PROVIDER_ID = "100"
DRAFTKINGS_PROVIDER_NAME = "DraftKings"
ET = ZoneInfo("America/New_York")

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; baseball_for_mat/1.0)",
        "Accept": "application/json",
    }
)


def current_time_context():
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)
    target_et_date = now_et.date()
    return now_utc, now_et, target_et_date, target_et_date.strftime("%Y_%m_%d")


def get_json(url, params=None):
    response = SESSION.get(url, params=params, timeout=30)
    if response.status_code != 200:
        print(f"GET error: {response.status_code} {response.url}")
        print(response.text)
        raise SystemExit(1)
    try:
        return response.json()
    except ValueError:
        print(f"Invalid JSON response: {response.url}")
        print(response.text[:2000])
        raise SystemExit(1)


def https_ref(value):
    if not value:
        return None
    value = str(value)
    if value.startswith("http://"):
        return "https://" + value[len("http://") :]
    return value


def parse_event_utc_datetime(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def parse_event_et_date(value):
    dt = parse_event_utc_datetime(value)
    return None if dt is None else dt.astimezone(ET).date()


def event_commence_value(event):
    return event.get("date") or event.get("commence_time")


def is_target_et_date(event, target_et_date):
    return parse_event_et_date(event_commence_value(event)) == target_et_date


def has_started(event, now_utc):
    dt = parse_event_utc_datetime(event_commence_value(event))
    return False if dt is None else dt <= now_utc


def to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def american_to_decimal(value):
    american = to_float(value)
    if american is None or american == 0:
        return None
    if american > 0:
        return round(1.0 + american / 100.0, 2)
    return round(1.0 + 100.0 / abs(american), 2)


def odds_decimal(value):
    if isinstance(value, dict):
        decimal_value = to_float(value.get("decimal"))
        if decimal_value is not None:
            return decimal_value
        american_value = value.get("american")
        if american_value is None:
            american_value = value.get("alternateDisplayValue")
        return american_to_decimal(american_value)
    return american_to_decimal(value)


def line_value(value):
    if isinstance(value, dict):
        for key in ("american", "alternateDisplayValue", "value"):
            parsed = to_float(value.get(key))
            if parsed is not None:
                return parsed
        return None
    return to_float(value)


def current_value(container, key):
    current = container.get("current") or {}
    if key in current and current.get(key) is not None:
        return current.get(key)
    return container.get(key)


def team_name(competitor, team_cache):
    team = competitor.get("team") or {}
    for key in ("displayName", "shortDisplayName", "name"):
        value = team.get(key)
        if value:
            return str(value)
    ref = https_ref(team.get("$ref"))
    if not ref:
        return None
    if ref not in team_cache:
        team_cache[ref] = get_json(ref)
    team_data = team_cache[ref]
    for key in ("displayName", "shortDisplayName", "name"):
        value = team_data.get(key)
        if value:
            return str(value)
    return None


def competition_sides(competition, team_cache):
    home_team = None
    away_team = None
    for competitor in competition.get("competitors") or []:
        name = team_name(competitor, team_cache)
        side = str(competitor.get("homeAway") or "").lower()
        if side == "home":
            home_team = name
        elif side == "away":
            away_team = name
    return home_team, away_team


def draftkings_odds_item(payload):
    items = payload.get("items") if isinstance(payload, dict) else None
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        return None
    for item in items:
        provider = item.get("provider") or {}
        provider_id = str(provider.get("id") or "")
        provider_name = str(
            provider.get("name") or provider.get("displayName") or ""
        )
        if (
            provider_id == DRAFTKINGS_PROVIDER_ID
            or provider_name.lower().replace(" ", "") == "draftkings"
        ):
            return item
    return None


def build_markets(odds, home_team, away_team, pulled_at):
    markets = []
    away = odds.get("awayTeamOdds") or {}
    home = odds.get("homeTeamOdds") or {}

    away_ml = odds_decimal(current_value(away, "moneyLine"))
    home_ml = odds_decimal(current_value(home, "moneyLine"))
    if away_ml is not None and home_ml is not None:
        markets.append(
            {
                "key": "h2h",
                "last_update": pulled_at,
                "outcomes": [
                    {"name": home_team, "price": home_ml},
                    {"name": away_team, "price": away_ml},
                ],
            }
        )

    away_rl = line_value(current_value(away, "pointSpread"))
    home_rl = line_value(current_value(home, "pointSpread"))
    away_rl_price = odds_decimal(current_value(away, "spread"))
    home_rl_price = odds_decimal(current_value(home, "spread"))
    if (
        away_rl is not None
        and home_rl is not None
        and away_rl_price is not None
        and home_rl_price is not None
    ):
        markets.append(
            {
                "key": "spreads",
                "last_update": pulled_at,
                "outcomes": [
                    {"name": home_team, "price": home_rl_price, "point": home_rl},
                    {"name": away_team, "price": away_rl_price, "point": away_rl},
                ],
            }
        )

    total = line_value(current_value(odds, "total"))
    over_price = odds_decimal(current_value(odds, "over"))
    under_price = odds_decimal(current_value(odds, "under"))
    if total is None:
        total = to_float(odds.get("overUnder"))
    if over_price is None:
        over_price = american_to_decimal(odds.get("overOdds"))
    if under_price is None:
        under_price = american_to_decimal(odds.get("underOdds"))
    if total is not None and over_price is not None and under_price is not None:
        markets.append(
            {
                "key": "totals",
                "last_update": pulled_at,
                "outcomes": [
                    {"name": "Over", "price": over_price, "point": total},
                    {"name": "Under", "price": under_price, "point": total},
                ],
            }
        )

    return markets


def fetch_espn_events(target_et_date, now_utc):
    target_compact = target_et_date.strftime("%Y%m%d")
    payload = get_json(
        f"{ESPN_BASE}/events",
        params={"dates": target_compact, "limit": 100},
    )
    refs = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(refs, list):
        print("Unexpected ESPN events response: missing items list")
        print(json.dumps(payload, indent=2)[:5000])
        raise SystemExit(1)

    team_cache = {}
    converted = []
    skipped_non_target = 0
    skipped_started = 0
    skipped_no_odds = 0
    skipped_incomplete = 0
    pulled_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    for entry in refs:
        event_ref = https_ref((entry or {}).get("$ref"))
        if not event_ref:
            skipped_incomplete += 1
            continue
        event = get_json(event_ref)
        competitions = event.get("competitions") or []
        if not competitions:
            skipped_incomplete += 1
            continue
        competition = competitions[0]
        commence_time = event.get("date") or competition.get("date")
        event_for_time = {"commence_time": commence_time}
        if not is_target_et_date(event_for_time, target_et_date):
            skipped_non_target += 1
            continue
        if has_started(event_for_time, now_utc):
            skipped_started += 1
            continue

        event_id = str(event.get("id") or competition.get("id") or "").strip()
        competition_id = str(competition.get("id") or event_id).strip()
        if not event_id or not competition_id:
            skipped_incomplete += 1
            continue

        home_team, away_team = competition_sides(competition, team_cache)
        if not home_team or not away_team:
            skipped_incomplete += 1
            continue

        odds_payload = get_json(
            f"{ESPN_BASE}/events/{event_id}/competitions/{competition_id}/odds"
        )
        odds = draftkings_odds_item(odds_payload)
        if not odds:
            skipped_no_odds += 1
            continue

        markets = build_markets(odds, home_team, away_team, pulled_at)
        if not markets:
            skipped_no_odds += 1
            continue

        converted.append(
            {
                "id": event_id,
                "sport_key": "baseball_mlb",
                "sport_title": "MLB",
                "commence_time": commence_time,
                "home_team": home_team,
                "away_team": away_team,
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "title": DRAFTKINGS_PROVIDER_NAME,
                        "last_update": pulled_at,
                        "markets": markets,
                    }
                ],
            }
        )

    return (
        converted,
        len(refs),
        skipped_non_target,
        skipped_started,
        skipped_no_odds,
        skipped_incomplete,
    )


def read_json_list(path):
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except json.JSONDecodeError:
        print(f"Invalid JSON in existing file: {path}")
        raise SystemExit(1)
    if not isinstance(loaded, list):
        print(f"Expected JSON list in existing file: {path}")
        raise SystemExit(1)
    return loaded


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
    temp_path.replace(path)


def get_event_id(event):
    value = event.get("id")
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def event_identity(event):
    home = str(event.get("home_team") or "").strip().lower()
    away = str(event.get("away_team") or "").strip().lower()
    dt = parse_event_utc_datetime(event.get("commence_time"))
    when = (
        dt.strftime("%Y-%m-%dT%H:%M")
        if dt
        else str(event.get("commence_time") or "")
    )
    return home, away, when


def sort_events(events):
    return sorted(
        events,
        key=lambda event: (
            event.get("commence_time", ""),
            event.get("home_team", ""),
            event.get("away_team", ""),
            event.get("id", ""),
        ),
    )


def find_matching_event_index(events, pulled):
    pulled_id = get_event_id(pulled)
    pulled_identity = event_identity(pulled)
    for index, existing in enumerate(events):
        same_id = pulled_id is not None and get_event_id(existing) == pulled_id
        same_game = event_identity(existing) == pulled_identity
        if same_id or same_game:
            return index
    return None


def print_pull_summary(
    *,
    target_et_date,
    date_string,
    now_utc,
    now_et,
    espn_events_found,
    skipped_non_target,
    skipped_started,
    skipped_no_odds,
    skipped_incomplete,
    converted_count,
    started_label,
):
    print(f"Target ET date: {target_et_date.isoformat()}")
    print(f"Target date string: {date_string}")
    print(f"Current UTC time: {now_utc.isoformat()}")
    print(f"Current ET time: {now_et.isoformat()}")
    print("Odds source: ESPN Core API")
    print(f"Sportsbook provider: {DRAFTKINGS_PROVIDER_NAME}")
    print(f"ESPN target-date event refs found: {espn_events_found}")
    print(f"Non-target ET date events skipped: {skipped_non_target}")
    print(f"{started_label}: {skipped_started}")
    print(f"Events skipped with no DraftKings odds: {skipped_no_odds}")
    print(f"Incomplete ESPN events skipped: {skipped_incomplete}")
    print(f"Events with converted odds this pull: {converted_count}")
