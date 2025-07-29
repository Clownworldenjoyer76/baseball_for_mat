import pandas as pd
from pathlib import Path
import sys

# File paths
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
FALLBACK_FILE = Path("data/end_chain/bat_today.csv")
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def safe_col(df, col, default=0):
    return df[col].fillna(default) if col in df.columns else pd.Series([default] * len(df))

def check_column(df, col, df_name):
    if col not in df.columns:
        print(f"❌ Column '{col}' NOT FOUND in {df_name}. Columns present: {list(df.columns)}")
        sys.exit(1)
    else:
        print(f"✅ Column '{col}' found in {df_name}")

def project_batter_props(df, pitchers, context, fallback):
    check_column(df, "last_name, first_name", f"{context} batters")
    check_column(pitchers, "last_name, first_name", "pitchers")
    check_column(fallback, "last_name, first_name", "fallback")

    key_col = "pitcher_away" if context == "home" else "pitcher_home"
    df["name_key_pitcher"] = df[key_col].astype(str).str.strip().str.lower()
    pitchers["name_key_pitcher"] = pitchers["last_name, first_name"].astype(str).str.strip().str.lower()

    df = df.merge(
        pitchers.drop(columns=["team_context", "team"], errors="ignore"),
        on="name_key_pitcher",
        how="left"
    )

    df["name_key_batter"] = df["last_name, first_name"].astype(str).str.strip().str.lower()
    fallback["name_key_batter"] = fallback["last_name, first_name"].astype(str).str.strip().str.lower()

    df = df.merge(
        fallback[["name_key_batter", "b_total_bases", "b_rbi"]],
        on="name_key_batter",
        how="left"
    )

    for col in ["b_total_bases", "b_rbi"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df["projected_total_bases"] = (
        safe_col(df, "adj_woba_combined", 0) * 1.75 +
        safe_col(df, "whiff%", 0) * -0.1 +
        safe_col(df, "zone_swing_miss%", 0) * -0.05 +
        safe_col(df, "out_of_zone_swing_miss%", 0) * -0.05 +
        safe_col(df, "gb%", 0) * -0.02 +
        safe_col(df, "fb%", 0) * 0.03 +
        safe_col(df, "innings_pitched", 0) * -0.01 +
        safe_col(df, "strikeouts", 0) * 0.005
    ).round(2)

    df["projected_hits"] = safe_col(df, "hit", 0).round(2)
    df["projected_walks"] = safe_col(df, "bb_percent", 0).round(2)
    df["projected_singles"] = (
        df["projected_hits"] -
        safe_col(df, "double", 0) -
        safe_col(df, "triple", 0) -
        safe_col(df, "home_run", 0)
    ).clip(lower=0).round(2)

    df["prop_type"] = "total_bases"
    df["context"] = context

    return df[[
        "last_name, first_name", "team", "projected_total_bases", "projected_hits",
        "projected_singles", "projected_walks", "b_rbi", "prop_type", "context"
    ]]

def main():
    print("🔄 Loading input files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    fallback = load_csv(FALLBACK_FILE)

    print("📊 Projecting props for home batters...")
    home_proj = project_batter_props(bat_home, pitchers, "home", fallback)

    print("📊 Projecting props for away batters...")
    away_proj = project_batter_props(bat_away, pitchers, "away", fallback)

    print("💾 Saving output...")
    all_proj = pd.concat([home_proj, away_proj], ignore_index=True)
    all_proj.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
