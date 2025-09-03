# scripts/combine_batter_weather_park.py

import pandas as pd
from pathlib import Path
import subprocess
import sys

REQ_WEATHER_COLS = {"player_id", "game_id", "adj_woba_weather"}
REQ_PARK_COLS    = {"player_id", "game_id", "adj_woba_park"}

def combine_adjustments(label: str) -> Path:
    weather_file = Path(f"data/adjusted/batters_{label}_weather.csv")
    park_file    = Path(f"data/adjusted/batters_{label}_park.csv")
    output_file  = Path(f"data/adjusted/batters_{label}_weather_park.csv")

    if not weather_file.exists():
        raise SystemExit(f"❌ missing: {weather_file}")
    if not park_file.exists():
        raise SystemExit(f"❌ missing: {park_file}")

    dfw = pd.read_csv(weather_file)
    dfp = pd.read_csv(park_file)

    if not REQ_WEATHER_COLS.issubset(dfw.columns):
        missing = REQ_WEATHER_COLS - set(dfw.columns)
        raise SystemExit(f"❌ {weather_file} missing columns: {sorted(missing)}")

    if not REQ_PARK_COLS.issubset(dfp.columns):
        missing = REQ_PARK_COLS - set(dfp.columns)
        raise SystemExit(f"❌ {park_file} missing columns: {sorted(missing)}")

    # Select minimal columns to avoid suffix collisions
    dfw_sel = dfw[["player_id", "game_id", "adj_woba_weather"]].copy()
    dfp_sel = dfp[["player_id", "game_id", "adj_woba_park"]].copy()

    merged = pd.merge(
        dfw_sel, dfp_sel,
        on=["player_id", "game_id"],
        how="inner",
        validate="one_to_one"
    )

    if merged.empty:
        print(f"⚠️ [{label}] merge produced 0 rows (check player_id/game_id alignment)")

    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2.0

    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_file, index=False)
    print(f"💾 [{label.upper()}] wrote: {output_file} ({len(merged)} rows)")

    return output_file

def commit_outputs(paths):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", *map(str, paths)], check=True)
        subprocess.run(["git", "commit", "-m", "combine_batter_weather_park: id-based merge"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ committed and pushed.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ git commit/push failed: {e}", file=sys.stderr)

def main():
    outputs = []
    for side in ("home", "away"):
        outputs.append(combine_adjustments(side))
    commit_outputs(outputs)

if __name__ == "__main__":
    main()
