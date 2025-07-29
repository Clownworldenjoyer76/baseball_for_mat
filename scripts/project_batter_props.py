# project_batter_props.py

import pandas as pd
from pathlib import Path

BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def project_batter_props(df, pitchers, context):
    key_col = "pitcher_away" if context == "home" else "pitcher_home"

    df["name_key"] = df[key_col].astype(str).str.strip().str.lower()
    pitchers["name_key"] = pitchers["last_name, first_name"].astype(str).str.strip().str.lower()

    df = df.merge(
        pitchers.drop(columns=["team_context"], errors="ignore"),
        on="name_key",
        how="left",
        suffixes=("", "_pitcher")
    )

    def safe_col(df, col, default=0):
        return df[col].fillna(0) if col in df.columns else pd.Series([default] * len(df))

    df["projected_total_bases"] = (
        df["adj_woba_combined"].fillna(0) * 1.75 +
        safe_col(df, "whiff%", 0) * -0.1 +
        safe_col(df, "zone_swing_miss%", 0) * -0.05 +
        safe_col(df, "out_of_zone_swing_miss%", 0) * -0.05 +
        safe_col(df, "gb%", 0) * -0.02 +
        safe_col(df, "fb%", 0) * 0.03 +
        safe_col(df, "innings_pitched", 0) * -0.01 +
        safe_col(df, "strikeouts", 0) * 0.005
    ).round(2)

    df["prop_type"] = "total_bases"
    df["context"] = context

    # Build output column list conditionally
    columns = ["name", "projected_total_bases", "prop_type", "context"]
    if "team" in df.columns:
        columns.insert(1, "team")

    return df[columns]

def main():
    print("🔄 Loading input files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)

    print("📊 Projecting props for home batters...")
    home_proj = project_batter_props(bat_home, pitchers, "home")

    print("📊 Projecting props for away batters...")
    away_proj = project_batter_props(bat_away, pitchers, "away")

    combined = pd.concat([home_proj, away_proj], ignore_index=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
