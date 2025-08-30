#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/get_weather_data.py

import pandas as pd
import requests
import time
from datetime import datetime
from pathlib import Path
from requests.exceptions import RequestException

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

INPUT_FILE   = "data/weather_input.csv"
OUTPUT_FILE  = "data/weather_adjustments.csv"
SCHED_FILE   = "data/bets/mlb_sched.csv"
MAP_FILE     = "data/Data/team_name_map.csv"
FORECAST_URL = "https://api.weatherapi.com/v1/forecast.json"
API_KEY      = "45d9502513854b489c3162411251907"

def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")

_GAME_TIME_FORMATS = [
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %I:%M %p",
    "%H:%M",
    "%I:%M %p",
]

def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{where}: missing columns {miss}")

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _build_team_map(path: str) -> dict:
    m = pd.read_csv(path)
    _require(m, ["name", "team"], path)
    return { _norm(n): str(t).strip() for n, t in zip(m["name"], m["team"]) }

def _canon(series: pd.Series, name_map: dict) -> pd.Series:
    return series.map(lambda s: name_map.get(_norm(s), str(s).strip()))

def parse_game_time_et(raw_time, raw_date=None):
    s = (str(raw_time) or "").strip()
    if not s or s.lower() in {"nan", "na"}:
        return None
    base_date = None
    if raw_date:
        for dfmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                base_date = datetime.strptime(str(raw_date).strip(), dfmt).date()
                break
            except Exception:
                continue
    if base_date is None:
        base_date = (datetime.now(ZoneInfo("America/New_York")).date()
                     if ZoneInfo else datetime.now().date())
    parsed_dt = None
    for fmt in _GAME_TIME_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ("%H:%M", "%I:%M %p"):
                dt = datetime.combine(base_date, dt.time())
            parsed_dt = dt
            break
        except Exception:
            continue
    if parsed_dt is None:
        return None
    return parsed_dt.replace(tzinfo=ZoneInfo("America/New_York")) if ZoneInfo else parsed_dt

def fetch_forecast(lat, lon, days=3):
    url = f"{FORECAST_URL}?key={API_KEY}&q={lat},{lon}&days={days}&aqi=no&alerts=no"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            print(f"{timestamp()} ⚠️ Rate limit. Sleep 5s.")
            time.sleep(5)
    except RequestException as e:
        print(f"{timestamp()} ⚠️ Request failed: {e}")
    return None

def pick_hour_block(forecast_json, target_local_dt, tzinfo):
    if not forecast_json:
        return None, None
    fdays = forecast_json.get("forecast", {}).get("forecastday", [])
    best, best_diff, best_day_str = None, None, None
    for day in fdays:
        for h in day.get("hour", []):
            try:
                t = pd.to_datetime(h.get("time")).to_pydatetime()
                if tzinfo is not None and t.tzinfo is None:
                    t = t.replace(tzinfo=tzinfo)
            except Exception:
                continue
            target_cmp = target_local_dt
            if (t.tzinfo is None) != (target_local_dt.tzinfo is None):
                t = t.replace(tzinfo=None) if t.tzinfo is not None else t
                target_cmp = (target_local_dt.replace(tzinfo=None)
                              if target_local_dt.tzinfo is not None
                              else target_local_dt)
            diff = abs((t - target_cmp).total_seconds())
            if best is None or diff < best_diff:
                best, best_diff = h, diff
                best_day_str = day.get("date")
    return best, best_day_str

def _attach_sched_keys(wx_df: pd.DataFrame, name_map: dict) -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["game_id", "date", "home_team", "away_team"], SCHED_FILE)
    sched["home_team"] = _canon(sched["home_team"], name_map)
    sched["away_team"] = _canon(sched["away_team"], name_map)
    # de-dup by teams; keep first per matchup/date
    sched = sched[["game_id", "date", "home_team", "away_team"]].drop_duplicates()
    # merge to get game_id/date into weather rows
    out = wx_df.merge(
        sched,
        on=["home_team", "away_team"],
        how="left",
        suffixes=("", "_sched")
    )
    # Ensure string keys
    out["game_id"] = out["game_id"].astype("string")
    out["date"]    = out["date"].astype("string")
    return out

def main():
    print(f"{timestamp()} 🔄 Read {INPUT_FILE}...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"{timestamp()} ❌ Failed to read {INPUT_FILE}: {e}")
        return

    df.columns = [c.strip() for c in df.columns]
    for col in ["venue", "city", "latitude", "longitude",
                "is_dome", "game_time", "home_team", "away_team"]:
        if col not in df.columns:
            print(f"{timestamp()} ⚠️ Missing column: {col}")

    name_map = _build_team_map(MAP_FILE)
    if "home_team" in df.columns:
        df["home_team"] = _canon(df["home_team"], name_map)
    if "away_team" in df.columns:
        df["away_team"] = _canon(df["away_team"], name_map)

    print(f"{timestamp()} 🌍 Fetch weather for {len(df)} venues...")
    rows = []

    for _, row in df.iterrows():
        venue = str(row.get("venue", "")).strip()
        city  = str(row.get("city", "")).strip()
        lat   = row.get("latitude", "")
        lon   = row.get("longitude", "")
        is_dome = row.get("is_dome", False)
        home_team = row.get("home_team", "UNKNOWN")
        away_team = row.get("away_team", "UNKNOWN")
        game_time_raw = row.get("game_time", "")
        game_date = row.get("game_date", None)

        if isinstance(is_dome, str):
            is_dome = is_dome.strip().lower() in {"true", "1", "yes", "y"}

        location = f"{venue}, {city}".strip(", ")

        dt_et = parse_game_time_et(game_time_raw, game_date)
        if dt_et is None:
            print(f"{timestamp()} ⚠️ Unparsable ET for {location}: '{game_time_raw}'.")
            dt_et = (datetime.now(ZoneInfo("America/New_York"))
                     if ZoneInfo else datetime.now())

        # default empty payload; will be filled on success
        record = {
            "venue": venue,
            "location": location,
            "matched_forecast_day": None,
            "matched_forecast_time": None,
            "temperature": None,
            "wind_speed": None,
            "wind_direction": None,
            "humidity": None,
            "precipitation": None,
            "condition": None,
            "notes": ("Roof closed" if is_dome else "Roof open"),
            "game_time_et": str(game_time_raw),
            "home_team": home_team,
            "away_team": away_team,
            "fetched_at": timestamp(),
        }

        # Skip API only if coords missing; still write stub row
        if not lat or not lon:
            print(f"{timestamp()} ⚠️ Missing coords for {location}. Stub row.")
            rows.append(record)
            continue

        # Fetch forecast
        attempts, data = 0, None
        while attempts < 5 and data is None:
            data = fetch_forecast(lat, lon, days=3)
            if data is None:
                attempts += 1
                time.sleep(1)

        if data is None:
            print(f"{timestamp()} ❌ No forecast for {location}. Stub row.")
            rows.append(record)
            continue

        tz_id = (data.get("location") or {}).get("tz_id")
        if ZoneInfo and tz_id:
            try:
                local_tz = ZoneInfo(tz_id)
            except Exception:
                local_tz = None
        else:
            local_tz = None

        dt_local = dt_et.astimezone(local_tz) if (local_tz and dt_et.tzinfo) else dt_et
        hour_block, matched_day = pick_hour_block(data, dt_local, local_tz)
        if not hour_block:
            print(f"{timestamp()} ⚠️ No hourly block for {location}. Stub row.")
            rows.append(record)
            continue

        cond = (hour_block.get("condition") or {}).get("text", "Unknown")
        precip_in = hour_block.get("precip_in")
        temp_f    = hour_block.get("temp_f")
        wind_mph  = hour_block.get("wind_mph")
        wind_dir  = hour_block.get("wind_dir")
        humidity  = hour_block.get("humidity")
        matched_time = hour_block.get("time")

        if is_dome:
            precip_in = 0.0
            wind_mph  = 0.0
            wind_dir  = "CALM"

        record.update({
            "matched_forecast_day": matched_day,
            "matched_forecast_time": matched_time,
            "temperature": temp_f,
            "wind_speed": wind_mph,
            "wind_direction": wind_dir,
            "humidity": humidity,
            "precipitation": precip_in,
            "condition": cond,
        })

        rows.append(record)

    if not rows:
        print(f"{timestamp()} ⚠️ No rows. Exit.")
        return

    wx = pd.DataFrame(rows)

    # Attach schedule keys (game_id, date) by canonical teams
    wx = _attach_sched_keys(wx, name_map)

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    wx.to_csv(OUTPUT_FILE, index=False)
    print(f"{timestamp()} ✅ Wrote {OUTPUT_FILE} with {len(wx)} rows.")

if __name__ == "__main__":
    main()
