#!/usr/bin/env python3
import pandas as pd
import requests
import time
from datetime import datetime, date, time as dtime
from requests.exceptions import RequestException

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # If unavailable, we’ll fallback to naive matching

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
API_KEY = "45d9502513854b489c3162411251907"
FORECAST_URL = "https://api.weatherapi.com/v1/forecast.json"  # hourly forecast

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")

_GAME_TIME_FORMATS = ["%Y-%m-%d %H:%M", "%Y-%m-%d %I:%M %p", "%m/%d/%Y %H:%M",
                      "%m/%d/%Y %I:%M %p", "%H:%M", "%I:%M %p"]

def parse_game_time_et(raw_time, raw_date=None):
    """
    Parse game_time given in ET. Returns an aware datetime in America/New_York
    if ZoneInfo is available; otherwise returns a naive ET datetime.
    """
    s = (str(raw_time) or "").strip()
    if not s or s.lower() in {"nan", "na"}:
        return None

    # Determine a base date in ET
    base_date = None
    if raw_date:
        for dfmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                base_date = datetime.strptime(str(raw_date).strip(), dfmt).date()
                break
            except Exception:
                continue
    if base_date is None:
        # default to "today" in ET
        base_date = datetime.now(ZoneInfo("America/New_York") if ZoneInfo else None).date()

    # Try all formats
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

    if ZoneInfo:
        return parsed_dt.replace(tzinfo=ZoneInfo("America/New_York"))
    else:
        # naive ET (best effort)
        return parsed_dt

def fetch_forecast(lat, lon, days=3):
    url = f"{FORECAST_URL}?key={API_KEY}&q={lat},{lon}&days={days}&aqi=no&alerts=no"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            print(f"{timestamp()} ⚠️ Rate limit hit. Sleeping 5 seconds.")
            time.sleep(5)
    except RequestException as e:
        print(f"{timestamp()} ⚠️ Request failed: {e}")
    return None

def pick_hour_block(forecast_json, target_local_dt):
    """Pick the hourly block closest to target_local_dt (local to venue)."""
    if not forecast_json:
        return None, None
    fdays = forecast_json.get("forecast", {}).get("forecastday", [])
    best = None
    best_diff = None
    best_day_str = None
    for day in fdays:
        for h in day.get("hour", []):
            try:
                t = pd.to_datetime(h.get("time")).to_pydatetime()
            except Exception:
                continue
            diff = abs((t - target_local_dt).total_seconds())
            if best is None or diff < best_diff:
                best, best_diff = h, diff
                best_day_str = day.get("date")
    return best, best_day_str

def main():
    print(f"{timestamp()} 🔄 Reading input file...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"{timestamp()} ❌ Failed to read input file: {e}")
        return

    # Normalize headers
    df.columns = [c.strip() for c in df.columns]
    expected = ["venue", "city", "latitude", "longitude", "is_dome",
                "game_time", "home_team", "away_team"]
    for col in expected:
        if col not in df.columns:
            print(f"{timestamp()} ⚠️ Missing column: {col}")

    print(f"{timestamp()} 🌍 Fetching weather for {len(df)} venues...")
    results = []

    for _, row in df.iterrows():
        venue = str(row.get("venue", "")).strip()
        city = str(row.get("city", "")).strip()
        lat = row.get("latitude", "")
        lon = row.get("longitude", "")
        is_dome = row.get("is_dome", False)
        home_team = row.get("home_team", "UNKNOWN")
        away_team = row.get("away_team", "UNKNOWN")
        game_time_raw = row.get("game_time", "")
        game_date = row.get("game_date", None)  # optional column

        # Coerce is_dome strings to bool
        if isinstance(is_dome, str):
            is_dome = is_dome.strip().lower() in {"true", "1", "yes", "y"}

        location = f"{venue}, {city}".strip(", ")

        if not lat or not lon:
            print(f"{timestamp()} ⚠️ Missing coordinates for {location}. Skipping.")
            continue

        # Parse ET start time (ALWAYS ET per your note)
        dt_et = parse_game_time_et(game_time_raw, game_date)
        if dt_et is None:
            print(f"{timestamp()} ⚠️ Unparsable ET game_time for {location}: '{game_time_raw}'. Using current ET.")
            dt_et = datetime.now(ZoneInfo("America/New_York") if ZoneInfo else None)

        # Fetch forecast (hourly) to get tz_id
        attempts, data = 0, None
        while attempts < 5 and data is None:
            data = fetch_forecast(lat, lon, days=3)
            if data is None:
                attempts += 1
                time.sleep(1)

        if data is None:
            print(f"{timestamp()} ❌ Failed to fetch forecast for {location} after 5 attempts.")
            continue

        tz_id = (data.get("location") or {}).get("tz_id")
        if ZoneInfo and tz_id:
            try:
                dt_local = dt_et.astimezone(ZoneInfo(tz_id))
            except Exception:
                dt_local = dt_et  # fallback
        else:
            # Fallback: assume ET == local if tz not available
            dt_local = dt_et

        hour_block, matched_day = pick_hour_block(data, dt_local)
        if not hour_block:
            print(f"{timestamp()} ⚠️ No hourly block found for {location} near {dt_local}.")
            continue

        condition_text = (hour_block.get("condition") or {}).get("text", "Unknown")
        precip_in = hour_block.get("precip_in")
        temp_f = hour_block.get("temp_f")
        wind_mph = hour_block.get("wind_mph")
        wind_dir = hour_block.get("wind_dir")
        humidity = hour_block.get("humidity")
        matched_time = hour_block.get("time")

        notes = "Roof closed" if is_dome else "Roof open"
        if is_dome:
            precip_in = 0.0
            wind_mph = 0.0
            wind_dir = "CALM"

        results.append({
            "venue": venue,
            "location": location,
            "matched_forecast_day": matched_day,
            "matched_forecast_time": matched_time,
            "temperature": temp_f,
            "wind_speed": wind_mph,
            "wind_direction": wind_dir,
            "humidity": humidity,
            "precipitation": precip_in,
            "condition": condition_text,
            "notes": notes,
            "game_time_et": str(game_time_raw),
            "home_team": home_team,
            "away_team": away_team,
            "fetched_at": timestamp()
        })

    if not results:
        print(f"{timestamp()} ⚠️ No weather data collected. Exiting.")
        return

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"{timestamp()} ✅ Weather data written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
