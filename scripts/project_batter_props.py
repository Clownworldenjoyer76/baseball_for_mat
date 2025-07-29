import pandas as pd
from pathlib import Path

# ✅ CORRECTED file paths
BAT_HOME_FILE = Path("data/end_chain/final/batter_home_final.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/batter_away_final.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
FALLBACK_FILE = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    try:
        return pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"Warning: UTF-8 decode failed for {path}, trying 'latin1'.")
        return pd.read_csv(path, encoding='latin1')

def safe_col(df, col, default=0):
    return df[col].fillna(default) if col in df.columns else pd.Series([default] * len(df))

def project_batter_props(df, pitchers, context, fallback):
    key_col = "pitcher_away" if context == "home" else "pitcher_home"

    print(f"\n--- Entering project_batter_props (context: {context}) ---")

    df.columns = df.columns.str.strip().str.replace(r'\s+', ' ', regex=True)
    pitchers.columns = pitchers.columns.str.strip().str.replace(r'\s+', ' ', regex=True)
    fallback.columns = fallback.columns.str.strip().str.replace(r'\s+', ' ', regex=True)

    print("Columns in input DF (bat_home/away) after cleaning:", df.columns.tolist())
    print("Columns in PITCHERS after cleaning:", pitchers.columns.tolist())
    print("Columns in FALLBACK after cleaning:", fallback.columns.tolist())

    expected_pitcher_name_col = "last_name, first_name"
    if expected_pitcher_name_col not in pitchers.columns:
        raise KeyError(f"Missing expected column: '{expected_pitcher_name_col}' in PITCHERS for merge.")

    pitchers_for_merge = pitchers.drop(columns=["team_context", "team"], errors="ignore")
    if expected_pitcher_name_col not in pitchers_for_merge.columns:
        raise KeyError(f"Missing expected column: '{expected_pitcher_name_col}' after drop in PITCHERS.")

    df = df.merge(
        pitchers_for_merge,
        left_on=key_col,
        right_on=expected_pitcher_name_col,
        how="left"
    )
    print("Columns in DF after first merge (Pitchers):", df.columns.tolist())

    expected_batter_id_in_df = "player_id_x"
    expected_batter_id_in_fallback = "player_id"
    if expected_batter_id_in_fallback not in fallback.columns or expected_batter_id_in_df not in df.columns:
        raise KeyError(f"Missing 'player_id' in fallback or 'player_id_x' in DF after merge.")

    df = df.merge(
        fallback[[expected_batter_id_in_fallback, "b_total_bases", "b_rbi"]],
        left_on=expected_batter_id_in_df,
        right_on=expected_batter_id_in_fallback,
        how="left"
    )
    print("Columns in DF after second merge (Fallback):", df.columns.tolist())

    for col in ["b_total_bases", "b_rbi"]:
        df[col] = df[col].fillna(0) if col in df.columns else 0

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

    if 'team' not in df.columns and 'away_team' in df.columns:
        df['team'] = df['away_team']

    df = df.rename(columns={
        "last_name, first_name_x": "last_name, first_name",
        "player_id_x": "player_id"
    })

    required_output_cols = [
        "player_id", "last_name, first_name", "team",
        "projected_total_bases", "projected_hits", "projected_singles",
        "projected_walks", "b_rbi", "prop_type", "context"
    ]
    return df[required_output_cols]

def main():
    print("🔄 Loading input files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    fallback = load_csv(FALLBACK_FILE)

    print("📊 Projecting props for home batters...")
    home_proj = project_batter_props(bat_home.copy(), pitchers.copy(), "home", fallback.copy())

    print("📊 Projecting props for away batters...")
    away_proj = project_batter_props(bat_away.copy(), pitchers.copy(), "away", fallback.copy())

    print("💾 Saving output...")
    all_proj = pd.concat([home_proj, away_proj], ignore_index=True)
    all_proj.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
