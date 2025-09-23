# scripts/combine_pitcher_weather_park_away.py

import pandas as pd
from pathlib import Path
import subprocess

WEATHER_FILE = "data/adjusted/pitchers_away_weather.csv"
PARK_FILE = "data/adjusted/pitchers_away_park.csv"
OUTPUT_FILE = "data/adjusted/pitchers_away_weather_park.csv"
LOG_FILE = "summaries/pitchers_adjust/log_pitchers_away_weather_park.txt"

REQUIRED_WEATHER_COLS = {"player_id", "adj_woba_weather"}
REQUIRED_PARK_COLS = {"player_id", "adj_woba_park"}
KEYS = ["player_id", "game_id"]

def _standardize_game_id_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # unify any game_id_* to game_id
    if "game_id" not in df.columns:
        for c in df.columns:
            if c.startswith("game_id_"):
                df.rename(columns={c: "game_id"}, inplace=True)
                break
    return df

def _validate_columns(df: pd.DataFrame, req: set, src: str) -> list[str]:
    missing = [c for c in req if c not in df.columns]
    notes = []
    if missing:
        notes.append(f"❌ {src} missing columns: {', '.join(missing)}")
    return notes

def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def main():
    log = []

    # Load
    try:
        weather = pd.read_csv(WEATHER_FILE)
        park = pd.read_csv(PARK_FILE)
    except Exception as e:
        Path(LOG_FILE).write_text(f"❌ Failed to read inputs: {e}\n")
        return

    # Standardize game_id columns
    weather = _standardize_game_id_cols(weather)
    park = _standardize_game_id_cols(park)

    # Validate keys exist
    need_keys_weather = [k for k in KEYS if k not in weather.columns]
    need_keys_park = [k for k in KEYS if k not in park.columns]
    if need_keys_weather:
        log.append(f"❌ WEATHER missing key columns: {', '.join(need_keys_weather)}")
    if need_keys_park:
        log.append(f"❌ PARK missing key columns: {', '.join(need_keys_park)}")

    # Validate value columns
    log += _validate_columns(weather, REQUIRED_WEATHER_COLS, "WEATHER")
    log += _validate_columns(park, REQUIRED_PARK_COLS, "PARK")

    if log:
        Path(LOG_FILE).write_text("\n".join(log) + "\n")
        # Still attempt to continue only if required keys and value cols exist
        if need_keys_weather or need_keys_park or any(s.startswith("❌") for s in log):
            # Hard stop if any required columns are missing
            return

    # Keep only required columns + keys + a minimal identifier for sanity checks
    weather_keep = list(set(KEYS) | REQUIRED_WEATHER_COLS | {"away_team"})
    park_keep = list(set(KEYS) | REQUIRED_PARK_COLS | {"away_team"})
    weather = weather[[c for c in weather_keep if c in weather.columns]].copy()
    park = park[[c for c in park_keep if c in park.columns]].copy()

    # Types
    _coerce_numeric(weather, ["player_id"])
    _coerce_numeric(park, ["player_id"])

    # Merge strictly on player_id + game_id
    merged = pd.merge(
        weather,
        park,
        on=KEYS,
        how="inner",
        suffixes=("_weather", "_park"),
    )

    # Compute combined adjustment (simple average to match prior behavior)
    if "adj_woba_weather" in merged.columns and "adj_woba_park" in merged.columns:
        _coerce_numeric(merged, ["adj_woba_weather", "adj_woba_park"])
        merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2.0

    # Reduce columns
    out_cols = [c for c in ["player_id", "game_id", "away_team_weather", "away_team_park",
                            "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]
                if c in merged.columns]
    # If only one away_team exists, keep it as away_team
    if "away_team_weather" in out_cols and "away_team_park" not in out_cols:
        merged.rename(columns={"away_team_weather": "away_team"}, inplace=True)
        out_cols = [("away_team" if c == "away_team_weather" else c) for c in out_cols]
    elif "away_team_park" in out_cols and "away_team_weather" not in out_cols:
        merged.rename(columns={"away_team_park": "away_team"}, inplace=True)
        out_cols = [("away_team" if c == "away_team_park" else c) for c in out_cols]

    out = merged[out_cols].copy()

    # Write outputs
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_FILE, index=False)

    # Log
    if out.empty:
        log.append("⚠️ Merge produced 0 rows. Check that pitchers_away_park.csv is populated and keys match (player_id + game_id).")
    else:
        top = out.sort_values(by="adj_woba_combined", ascending=False, na_position="last").head(5)
        log.append("Top 5 by adj_woba_combined:")
        log.append(top.to_string(index=False))

    Path(LOG_FILE).write_text("\n".join(log) + "\n")

    # Commit
    try:
        subprocess.run(["git", "add", OUTPUT_FILE, LOG_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Combine pitcher weather+park (away) on player_id+game_id"], check=True)
        subprocess.run(["git", "push"], check=True)
    except Exception:
        # Silent on mobile; commit may be skipped in CI
        pass

if __name__ == "__main__":
    main()
