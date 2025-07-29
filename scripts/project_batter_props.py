import pandas as pd
from pathlib import Path

# File paths
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
FALLBACK_FILE = Path("data/end_chain/bat_today.csv")  # updated path
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    # Try different encodings if default fails, though 'utf-8' is usually fine
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

    # --- Aggressive Column Name Cleaning for All DataFrames ---
    # Apply to df (bat_home/away)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True) # Replace multiple spaces with single space

    # Apply to pitchers
    pitchers.columns = pitchers.columns.str.strip()
    pitchers.columns = pitchers.columns.str.replace(r'\s+', ' ', regex=True)

    # Apply to fallback
    fallback.columns = fallback.columns.str.strip()
    fallback.columns = fallback.columns.str.replace(r'\s+', ' ', regex=True)
    # --- End of Aggressive Cleaning ---


    print("Columns in input DF (bat_home/away) after cleaning:", df.columns.tolist())
    print("Columns in PITCHERS after cleaning:", pitchers.columns.tolist())
    print("Columns in FALLBACK after cleaning:", fallback.columns.tolist())

    # --- Explicit Check before the first merge (Pitchers) ---
    # This merge still uses 'last_name, first_name' as the key_col (pitcher name) in batter data
    # (pitcher_home/away) is a name, not an ID.
    expected_pitcher_name_col = "last_name, first_name"
    if expected_pitcher_name_col not in pitchers.columns:
        print(f"\nCRITICAL ERROR: '{expected_pitcher_name_col}' still not found in PITCHERS DataFrame after cleaning!")
        print(f"Actual columns in PITCHERS: {pitchers.columns.tolist()}")
        raise KeyError(f"Missing expected column: '{expected_pitcher_name_col}' in PITCHERS for merge.")

    # Match on pitcher name
    pitchers_for_merge = pitchers.drop(columns=["team_context", "team"], errors="ignore")
    print("Columns in PITCHERS AFTER drop for merge 1:", pitchers_for_merge.columns.tolist())

    # Re-check after dropping just to be absolutely certain it wasn't dropped somehow (highly unlikely with errors='ignore')
    if expected_pitcher_name_col not in pitchers_for_merge.columns:
        print(f"\nCRITICAL ERROR: '{expected_pitcher_name_col}' *still* not found in PITCHERS AFTER DROP!")
        print(f"Actual columns in PITCHERS after drop: {pitchers_for_merge.columns.tolist()}")
        raise KeyError(f"Missing expected column: '{expected_pitcher_name_col}' in PITCHERS after drop for merge.")


    df = df.merge(
        pitchers_for_merge,
        left_on=key_col,
        right_on=expected_pitcher_name_col, # Still merging on pitcher name here
        how="left"
    )
    print("Columns in DF after first merge (Pitchers):", df.columns.tolist())


    # --- Explicit Check before the second merge (Fallback - now on player_id) ---
    # The 'player_id' column from the initial 'df' (batter data) will become 'player_id_x' after the first merge.
    # The 'player_id' from the 'fallback' dataframe will remain 'player_id'.
    expected_batter_id_in_df = "player_id_x" # The batter's player_id after the first merge
    expected_batter_id_in_fallback = "player_id" # The batter's player_id in the fallback file

    if expected_batter_id_in_fallback not in fallback.columns:
        print(f"\nCRITICAL ERROR: '{expected_batter_id_in_fallback}' still not found in FALLBACK DataFrame after cleaning!")
        print(f"Actual columns in FALLBACK: {fallback.columns.tolist()}")
        raise KeyError(f"Missing expected column: '{expected_batter_id_in_fallback}' in FALLBACK for merge.")
    
    if expected_batter_id_in_df not in df.columns:
        print(f"\nCRITICAL ERROR: '{expected_batter_id_in_df}' still not found in DF (bat_home/away) for fallback merge!")
        print(f"Actual columns in DF: {df.columns.tolist()}")
        raise KeyError(f"Missing expected column: '{expected_batter_id_in_df}' in DF for fallback merge.")


    # Match on batter player_id
    df = df.merge(
        fallback[[expected_batter_id_in_fallback, "b_total_bases", "b_rbi"]],
        left_on=expected_batter_id_in_df,    # Use 'player_id_x' from the main DF
        right_on=expected_batter_id_in_fallback, # Use 'player_id' from the fallback DF
        how="left"
    )
    print("Columns in DF after second merge (Fallback):", df.columns.tolist())

    for col in ["b_total_bases", "b_rbi"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)
        else:
            print(f"Warning: Column '{col}' not found after merges to fillna. Creating with default 0.")
            df[col] = 0 # Ensure it exists if somehow not merged

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

    # Handle 'team' column consistency for output
    if 'team' not in df.columns and 'away_team' in df.columns:
        df['team'] = df['away_team']
    # If 'team' is already there (from home data), we assume it's the correct one.
    # If it's the away context, and 'team' isn't explicitly defined, 'away_team' is used.
    # If 'home_team' is the batter's team for away context, a different logic is needed.
    # Assuming 'away_team' is the batter's team when context is 'away'.


    # Final columns check before returning
    # Rename for clarity in output. We prioritize the batter's information.
    # The player_id and last_name, first_name from the initial batter DF are player_id_x and last_name, first_name_x
    df = df.rename(columns={
        "last_name, first_name_x": "last_name, first_name",
        "player_id_x": "player_id"
    })

    required_output_cols = [
        "player_id",
        "last_name, first_name",
        "team", # Now ensured to exist
        "projected_total_bases",
        "projected_hits",
        "projected_singles",
        "projected_walks",
        "b_rbi",
        "prop_type",
        "context"
    ]
    missing_output_cols = [col for col in required_output_cols if col not in df.columns]
    if missing_output_cols:
        print(f"WARNING: Missing expected output columns: {missing_output_cols}")
        print(f"Available columns before return: {df.columns.tolist()}")

    return df[required_output_cols]

def main():
    print("🔄 Loading input files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    fallback = load_csv(FALLBACK_FILE)

    print("\n--- Initial Load Checks (before any processing) ---")
    print("Bat Home Columns (initial):", bat_home.columns.tolist())
    print("Bat Away Columns (initial):", bat_away.columns.tolist())
    print("Pitchers Columns (initial):", pitchers.columns.tolist())
    print("Fallback Columns (initial):", fallback.columns.tolist())
    print("---------------------------------------------------\n")

    print("📊 Projecting props for home batters...")
    # Pass copies to avoid modifying original DFs if processed multiple times in the same run
    home_proj = project_batter_props(bat_home.copy(), pitchers.copy(), "home", fallback.copy())

    print("📊 Projecting props for away batters...")
    away_proj = project_batter_props(bat_away.copy(), pitchers.copy(), "away", fallback.copy())

    print("💾 Saving output...")
    all_proj = pd.concat([home_proj, away_proj], ignore_index=True)
    all_proj.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Projections saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
