# scripts/combine_pitcher_weather_park_home.py

import pandas as pd
import subprocess
from pathlib import Path

WEATHER_FILE = "data/adjusted/pitchers_home_weather.csv"
PARK_FILE    = "data/adjusted/pitchers_home_park.csv"
OUTPUT_FILE  = "data/adjusted/pitchers_home_weather_park.csv"
LOG_FILE     = "summaries/pitchers_adjust/log_pitchers_home_weather_park.txt"

REQUIRED_WEATHER_COLS = {"player_id", "game_id", "adj_woba_weather"}
REQUIRED_PARK_COLS    = {"player_id", "game_id", "adj_woba_park"}
OUTPUT_COLS           = ["player_id", "game_id", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]


def write_log(lines):
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")


def safe_to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def main():
    # Ensure log dir exists up front
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    logs = []
    try:
        weather = pd.read_csv(WEATHER_FILE)
        park    = pd.read_csv(PARK_FILE)
        logs.append(f"Loaded {WEATHER_FILE} ({len(weather)} rows)")
        logs.append(f"Loaded {PARK_FILE} ({len(park)} rows)")
    except Exception as e:
        write_log([f"❌ Failed to read inputs: {e}"])
        # Write empty output with headers so downstream doesn't explode
        pd.DataFrame(columns=OUTPUT_COLS).to_csv(OUTPUT_FILE, index=False)
        return

    missing_w = REQUIRED_WEATHER_COLS - set(weather.columns)
    missing_p = REQUIRED_PARK_COLS - set(park.columns)

    if missing_w or missing_p:
        lines = []
        if missing_w:
            lines.append(f"INSUFFICIENT INFORMATION: Missing columns in {WEATHER_FILE}: {sorted(missing_w)}")
        if missing_p:
            lines.append(f"INSUFFICIENT INFORMATION: Missing columns in {PARK_FILE}: {sorted(missing_p)}")
        # Write empty output with headers for downstream stability
        pd.DataFrame(columns=OUTPUT_COLS).to_csv(OUTPUT_FILE, index=False)
        write_log(lines)
        return

    # Keep only required columns (in deterministic order)
    weather = weather[["player_id", "game_id", "adj_woba_weather"]].copy()
    park    = park[["player_id", "game_id", "adj_woba_park"]].copy()

    # Ensure numeric for combination
    weather = safe_to_numeric(weather, ["player_id", "game_id", "adj_woba_weather"])
    park    = safe_to_numeric(park,    ["player_id", "game_id", "adj_woba_park"])

    merged = pd.merge(
        weather,
        park,
        on=["player_id", "game_id"],
        how="inner",
        validate="one_to_one"
    )

    if merged.empty:
        logs.append("⚠️ Merge produced 0 rows on ['player_id','game_id'].")

    # Compute combined adjustment
    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2.0

    # Order/limit columns
    merged = merged[OUTPUT_COLS]

    # Save
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    logs.append(f"✅ Wrote {OUTPUT_FILE} ({len(merged)} rows)")

    # Log top 5 if available
    if "adj_woba_combined" in merged.columns and not merged.empty:
        top5 = merged.sort_values("adj_woba_combined", ascending=False).head(5)
        logs.append("Top 5 by adj_woba_combined:")
        logs.append(top5.to_string(index=False))

    write_log(logs)

    # Commit (best-effort in CI)
    try:
        subprocess.run(["git", "add", OUTPUT_FILE, LOG_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Combine pitcher weather+park (home) on player_id+game_id"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError:
        # No-op if nothing to commit or push fails in CI
        pass


if __name__ == "__main__":
    main()
