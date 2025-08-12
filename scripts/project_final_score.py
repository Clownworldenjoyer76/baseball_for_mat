#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

# Config
TARGET_AVG_TOTAL = 9.0
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
GAMES_FILE = Path("data/cleaned/games_today_cleaned.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def _norm_team(name):
    return str(name).strip().lower()

def _norm_name(name):
    return str(name).strip().lower()

def load_strengths():
    batters = pd.read_csv(BATTER_FILE)
    batters["team"] = batters["team"].map(_norm_team)
    # Keep only total_bases props
    bats_tb = batters[batters["prop_type"] == "total_bases"].copy()
    # Top 9 per team
    bat_strength = (
        bats_tb.groupby("team")["projection"]
        .apply(lambda s: np.mean(sorted(s, reverse=True)[:9]))
        .to_dict()
    )

    pitchers = pd.read_csv(PITCHER_FILE)
    pitchers["name"] = pitchers["name"].map(_norm_name)
    # Use mega_z as pitcher strength
    pitch_strength = pitchers.set_index("name")["mega_z"].to_dict()

    print(f"Bat strength keys: {len(bat_strength)}, Pitch strength keys: {len(pitch_strength)}")
    return bat_strength, pitch_strength

def main():
    bat_strength, pitch_strength = load_strengths()
    games = pd.read_csv(GAMES_FILE)
    games["home_team"] = games["home_team"].map(_norm_team)
    games["away_team"] = games["away_team"].map(_norm_team)
    if "date" not in games.columns or games["date"].isna().all():
        games["date"] = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Merge weather
    if WEATHER_FILE.exists():
        weather = pd.read_csv(WEATHER_FILE)
        weather["home_team"] = weather["home_team"].map(_norm_team)
        if "weather_factor" in weather.columns:
            games = games.merge(
                weather[["home_team", "weather_factor"]],
                on="home_team",
                how="left"
            )
        else:
            games["weather_factor"] = 1.0
    else:
        games["weather_factor"] = 1.0

    games["weather_factor"] = pd.to_numeric(games["weather_factor"], errors="coerce").fillna(1.0)

    home_scores, away_scores = [], []
    for _, row in games.iterrows():
        bh = bat_strength.get(row["home_team"], 0.0)
        pa = pitch_strength.get(_norm_name(row["away_pitcher"]), 0.0)
        ba = bat_strength.get(row["away_team"], 0.0)
        ph = pitch_strength.get(_norm_name(row["home_pitcher"]), 0.0)

        wf = row["weather_factor"]
        hs = 4.5 + (bh - pa)
        as_ = 4.5 + (ba - ph)

        hs *= np.sqrt(wf)
        as_ *= np.sqrt(wf)

        home_scores.append(hs)
        away_scores.append(as_)

    games["home_score"] = home_scores
    games["away_score"] = away_scores

    # Scale totals if needed
    avg_total = (games["home_score"] + games["away_score"]).mean()
    if len(games) >= 6 and (avg_total < 8.2 or avg_total > 9.8):
        scale_factor = TARGET_AVG_TOTAL / avg_total
        games["home_score"] *= scale_factor
        games["away_score"] *= scale_factor

    games["home_score"] = games["home_score"].round(2)
    games["away_score"] = games["away_score"].round(2)
    games["total"] = (games["home_score"] + games["away_score"]).round(2)

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
