import pandas as pd
from utils import safe_col

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    print("\n--- DEBUG: Inside calculate_projected_total_bases ---")
    print(f"DEBUG: Columns available: {df.columns.tolist()}")

    df["projected_total_bases"] = (
        safe_col(df, "adj_woba_combined", 0) * 1.75 +
        safe_col(df, "whiff_percent", 0) * -0.08 +
        safe_col(df, "zone_swing_miss_percent", 0) * -0.04 +
        safe_col(df, "out_of_zone_swing_miss_percent", 0) * -0.04 +
        safe_col(df, "gb_percent", 0) * -0.01 +
        safe_col(df, "fb_percent", 0) * 0.04 +
        safe_col(df, "innings_pitched", 0) * -0.01 +
        safe_col(df, "strikeouts", 0) * 0.002
    ).clip(lower=0).round(2)
    return df

def calculate_projected_hits(df: pd.DataFrame) -> pd.DataFrame:
    df["projected_hits"] = safe_col(df, "hit", 0).round(2)
    return df

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    df["projected_walks"] = (safe_col(df, "bb_percent", 0) / 100 * 4).round(2)
    return df

def calculate_projected_singles(df: pd.DataFrame) -> pd.DataFrame:
    df["projected_singles"] = (
        safe_col(df, "projected_hits", 0) -
        safe_col(df, "double", 0) -
        safe_col(df, "triple", 0) -
        safe_col(df, "home_run", 0)
    ).clip(lower=0).round(2)
    return df

def calculate_projected_rbi(df: pd.DataFrame) -> pd.DataFrame:
    df["projected_rbi"] = safe_col(df, "b_rbi", 0).round(2)
    return df

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_projected_total_bases(df)
    df = calculate_projected_hits(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_singles(df)
    df = calculate_projected_rbi(df)
    return df
