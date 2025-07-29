# data_preprocessing.py
import pandas as pd
# --- CHANGE THIS IMPORT ---
# From: from .utils import standardize_name_key
# To:   from utils import standardize_name_key
from utils import standardize_name_key

def merge_with_pitcher_data(batters_df: pd.DataFrame, pitchers_df: pd.DataFrame, context: str) -> pd.DataFrame:
    """
    Merges batter DataFrame with relevant pitcher data.
    """
    df_copy = batters_df.copy()
    pitchers_copy = pitchers_df.copy()

    key_col = "pitcher_away" if context == "home" else "pitcher_home"
    df_copy = standardize_name_key(df_copy, key_col)
    pitchers_copy = standardize_name_key(pitchers_copy, "last_name, first_name")

    merged_df = df_copy.merge(
        pitchers_copy.drop(columns=["team_context", "team"], errors="ignore"),
        on="name_key",
        how="left"
    )
    return merged_df

def apply_batter_fallback_stats(main_df: pd.DataFrame, fallback_df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies fallback statistics (b_total_bases, b_rbi) from a fallback DataFrame.
    """
    df_copy = main_df.copy()
    fallback_copy = fallback_df.copy()

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

    return merged_df
