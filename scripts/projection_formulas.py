# projection_formulas.py
import pandas as pd
from utils import safe_col

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    for col in [
        "adj_woba_combined", "whiff_percent", "zone_swing_miss_percent",
        "out_of_zone_swing_miss_percent", "gb_percent", "fb_percent",
        "innings_pitched", "strikeouts"
    ]:
        df_copy[col] = safe_col(df_copy, col, 0)

    df_copy["ip_per_start"] = df_copy["innings_pitched"] / 20.0
    df_copy["k_per_9"] = df_copy["strikeouts"] / (df_copy["innings_pitched"] / 9.0).replace(0, 1)

    df_copy["projected_total_bases"] = (
        df_copy["adj_woba_combined"] * 2.5 +
        df_copy["fb_percent"] * 0.05 +
        df_copy["gb_percent"] * -0.01 +
        df_copy["whiff_percent"] * -0.02 +
        df_copy["zone_swing_miss_percent"] * -0.015 +
        df_copy["out_of_zone_swing_miss_percent"] * -0.015 +
        df_copy["ip_per_start"] * -0.15 +
        df_copy["k_per_9"] * -0.05
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_hits(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    df_copy["projected_hits"] = (
        safe_col(df_copy, "hit", 0) * 0.6 +
        safe_col(df_copy, "babip", 0) * 0.5 -
        (safe_col(df_copy, "walks", 0) / (safe_col(df_copy, "innings_pitched", 1))) * 0.5
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    pitcher_walk_rate = safe_col(df_copy, "walks", 0) / (safe_col(df_copy, "innings_pitched", 1))
    df_copy["projected_walks"] = (
        safe_col(df_copy, "bb_percent", 0) * 0.75 -
        pitcher_walk_rate * 0.25
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_singles(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    df_copy["projected_singles"] = (
        safe_col(df_copy, "projected_hits", 0) -
        safe_col(df_copy, "double", 0) -
        safe_col(df_copy, "triple", 0) -
        safe_col(df_copy, "home_run", 0)
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_rbi(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    df_copy["projected_rbi"] = (
        safe_col(df_copy, "home_run", 0) * 1.2 +
        safe_col(df_copy, "hit", 0) * 0.3 +
        safe_col(df_copy, "b_rbi", 0) * 0.6
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_home_runs(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    # Estimate HR/9 for pitcher
    df_copy["hr_9"] = safe_col(df_copy, "home_run", 0) / (safe_col(df_copy, "innings_pitched", 1) / 9.0)

    df_copy["projected_home_runs"] = (
        safe_col(df_copy, "home_run", 0) * 0.6 +
        safe_col(df_copy, "fb_percent", 0) * 0.05 -
        df_copy["hr_9"] * 0.4
    ).clip(lower=0).round(2)

    return df_copy

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_projected_total_bases(df)
    df = calculate_projected_hits(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_singles(df)
    df = calculate_projected_rbi(df)
    df = calculate_projected_home_runs(df)
    return df
