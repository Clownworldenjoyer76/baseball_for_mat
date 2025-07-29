import pandas as pd
from pathlib import Path

BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
BATTER_FALLBACK_FILE = Path("data/cleaned/batters_normalized_cleaned.csv")
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def project_batter_props(df, pitchers, context, fallback_data):
    key_col = "pitcher_away" if context == "home" else "pitcher_home"

    df["name_key"] = df[key_col].astype(str).str.strip().str.lower()
    pitchers["name_key"] = pitchers["last_name, first_name"].astype(str).str.strip().str.lower()

    df = df.merge(
        pitchers.drop(columns=["team_context", "team"], errors="ignore"),
        on="name_key",
        how="left",
        suffixes=("", "_pitcher")
    )

    # Create safe column getter
    def safe_col(df_, col, default=0):
        return df_[col].fillna(default) if col in df_.columns else pd.Series([default] * len(df_))

    # Fill b_total_bases and b_rbi if missing from fallback
    fallback_data["name_key"] = fallback_data["name"].astype(str).str.strip().str.lower()

    for col in ["b_total_bases", "b_rbi"]:
        if col not in df.columns or df[col].isna().all():
            df = df.merge(
                fallback_data[["name_key", col]],
                on="name_key",
                how="left",
                suffixes=("", "_fallback")
            )
            df[col] = df[col].fillna(df[f"{col}_fallback"])
            df.drop(columns=[f"{col}_fallback"], inplace=True)

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

    # Additional props
    df["projected_hits"] = df["hit"].fillna(0).astype(float).round(2)
    df["projected_walks"] = df["bb_percent"].fillna(0).astype(float).round(2)
    df["projected_rbi"] = df["b_rbi"].fillna(0).astype(float).round(2)
    df["projected_tb"] = df["b_total_bases"].fillna(0).astype(float).round(2)

    df["context"] = context

    return df[[
        "name", "team", "projected_total_bases",
        "projected_hits", "projected_walks", "projected_rbi", "projected_tb", "context"
    ]]

def main():
    print("🔄 Loading input files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    fallback_data = load_csv(BATTER_FALLBACK_FILE)

    print("📊 Projecting props for home batters...")
    home_proj = project_batter_props(bat_home, pitchers, "home", fallback_data)

    print("📊 Projecting props for away batters...")
    away_proj = project_batter_props(bat_away, pitchers, "away", fallback_data)

    combined = pd.concat([home_proj, away_proj], ignore_index=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
