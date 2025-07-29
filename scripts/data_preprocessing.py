# data_preprocessing.py
import pandas as pd
from utils import standardize_name_key # Assuming utils.py is in the same directory

def merge_with_pitcher_data(batters_df: pd.DataFrame, pitchers_df: pd.DataFrame, context: str) -> pd.DataFrame:
    """
    Merges batter DataFrame with relevant pitcher data and normalizes team column.
    """
    df_copy = batters_df.copy()
    pitchers_copy = pitchers_df.copy()

    # --- ADD THIS DEBUG PRINT ---
    print(f"\n--- DEBUG: Inside merge_with_pitcher_data (context={context}) ---")
    print(f"DEBUG: Columns in batters_df BEFORE merge: {df_copy.columns.tolist()}")
    print(f"DEBUG: Columns in pitchers_df BEFORE merge: {pitchers_copy.columns.tolist()}")
    print("DEBUG: Sample pitchers_df 'name_key' (first 5):")
    # Make sure 'name_key' is generated before printing for pitchers_copy
    pitchers_copy_with_key = standardize_name_key(pitchers_copy.copy(), "last_name, first_name")
    print(pitchers_copy_with_key['name_key'].head())
    print("DEBUG: Sample batters_df 'name_key' (first 5):")
    # Make sure 'name_key' is generated before printing for df_copy
    df_copy_with_key = standardize_name_key(df_copy.copy(), "pitcher_away" if context == "home" else "pitcher_home")
    print(df_copy_with_key['name_key'].head())
    # --- END ADDITION ---

    key_col = "pitcher_away" if context == "home" else "pitcher_home"
    df_copy = standardize_name_key(df_copy, key_col)
    pitchers_copy = standardize_name_key(pitchers_copy, "last_name, first_name")

    if context == "away":
        if 'away_team' in df_copy.columns:
            df_copy = df_copy.rename(columns={"away_team": "team"})
        if 'team' in df_copy.columns:
            df_copy['team'] = df_copy['team'].astype(str).str.title()
    elif context == "home":
        if 'team' in df_copy.columns:
            df_copy['team'] = df_copy['team'].astype(str).str.title()

    merged_df = df_copy.merge(
        pitchers_copy.drop(columns=["team_context", "team"], errors="ignore"),
        on="name_key",
        how="left"
    )
    # --- ADD THIS DEBUG PRINT ---
    print(f"DEBUG: Columns in merged_df AFTER pitcher merge (context={context}): {merged_df.columns.tolist()}")
    print("DEBUG: First 5 rows of merged_df after pitcher merge (relevant columns):")
    # Print a sample of merged_df to see if data came over, including relevant pitcher stats
    cols_to_check = ['name_key', 'adj_woba_combined', 'whiff%', 'innings_pitched', 'strikeouts', 'team']
    # Filter for columns that actually exist to prevent a new KeyError here
    existing_cols_to_check = [col for col in cols_to_check if col in merged_df.columns]
    print(merged_df[existing_cols_to_check].head())
    print("--- END DEBUG: After pitcher merge ---\n")
    # --- END ADDITION ---

    return merged_df

def apply_batter_fallback_stats(main_df: pd.DataFrame, fallback_df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies fallback statistics (b_total_bases, b_rbi) from a fallback DataFrame.
    """
    df_copy = main_df.copy()
    fallback_copy = fallback_df.copy()

    # --- ADD THIS DEBUG PRINT ---
    print("\n--- DEBUG: Inside apply_batter_fallback_stats ---")
    print(f"DEBUG: Columns in main_df BEFORE fallback merge: {df_copy.columns.tolist()}")
    print(f"DEBUG: Columns in fallback_df BEFORE fallback merge: {fallback_copy.columns.tolist()}")
    print("DEBUG: Sample fallback_df 'batter_name' (first 5):")
    print(fallback_copy['name'].head()) # Assuming 'name' is original col in fallback
    # --- END ADDITION ---

    df_copy = df_copy.rename(columns={"name": "batter_name"})
    fallback_copy = fallback_copy.rename(columns={"name": "batter_name"})
    fallback_trimmed = fallback_copy[["batter_name", "b_total_bases", "b_rbi"]]

    merged_df = df_copy.merge(fallback_trimmed, on="batter_name", how="left", suffixes=('_current', '_fallback'))

    for col in ["b_total_bases", "b_rbi"]:
        if f"{col}_fallback" in merged_df.columns:
            merged_df[col] = merged_df[f"{col}_current"].combine_first(merged_df[f"{col}_fallback"])
            merged_df = merged_df.drop(columns=[f"{col}_current", f"{col}_fallback"])
        elif col not in merged_df.columns and f"{col}_fallback" in merged_df.columns:
             merged_df[col] = merged_df[f"{col}_fallback"]

    # --- ADD THIS DEBUG PRINT ---
    print(f"DEBUG: Columns in merged_df AFTER fallback merge: {merged_df.columns.tolist()}")
    print("DEBUG: First 5 rows of merged_df after fallback merge (relevant columns):")
    # Check adj_woba_combined here, and b_total_bases/b_rbi
    cols_to_check = ['batter_name', 'adj_woba_combined', 'b_total_bases', 'b_rbi', 'team']
    existing_cols_to_check = [col for col in cols_to_check if col in merged_df.columns]
    print(merged_df[existing_cols_to_check].head())
    print("--- END DEBUG: After fallback merge ---\n")
    # --- END ADDITION ---

    return merged_df
