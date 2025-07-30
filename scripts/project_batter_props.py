import pandas as pd
from utils import safe_col

# Assumed average plate appearances per game (adjustable)
PLATE_APPEARANCES_PER_GAME = 4.2

def clip_range(series, min_val, max_val):
    return series.clip(lower=min_val, upper=max_val)

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    df_copy["projected_total_bases"] = (
        safe_col(df_copy, "adj_woba_combined", 0) * 1.75 +
        safe_col(df_copy, "whiff_percent", 0) * -0.08 +
        safe_col(df_copy, "zone_swing_miss_percent", 0) * -0.04 +
        safe_col(df_copy, "out_of_zone_swing_miss_percent", 0) * -0.04 +
        safe_col(df_copy, "gb_percent", 0) * -0.01 +
        safe_col(df_copy, "fb_percent", 0) * 0.04 +
        safe_col(df_copy, "innings_pitched", 0) * -0.01 +
        safe_col(df_copy, "strikeouts", 0) * 0.002
    )
    df_copy["projected_total_bases"] = clip_range(df_copy["projected_total_bases"], 0, 6)
    return df_copy

def calculate_projected_hits(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    hits_per_game = safe_col(df_copy, "hit", 0) / safe_col(df_copy, "games", PLATE_APPEARANCES_PER_GAME)
    df_copy["projected_hits"] = clip_range(hits_per_game, 0, 5)
    return df_copy

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    walks_per_game = (safe_col(df_copy, "bb_percent", 0) / 100) * PLATE_APPEARANCES_PER_GAME
    df_copy["projected_walks"] = clip_range(walks_per_game, 0, 3)
    return df_copy

def calculate_projected_singles(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["projected_singles"] = (
        safe_col(df_copy, "projected_hits", 0) -
        safe_col(df_copy, "double", 0) -
        safe_col(df_copy, "triple", 0) -
        safe_col(df_copy, "home_run", 0)
    ).clip(lower=0, upper=5)
    return df_copy

def calculate_projected_rbi(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    rbi_per_game = safe_col(df_copy, "b_rbi", 0) / safe_col(df_copy, "games", PLATE_APPEARANCES_PER_GAME)
    df_copy["projected_rbi"] = clip_range(rbi_per_game, 0, 6)
    return df_copy

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_projected_total_bases(df)
    df = calculate_projected_hits(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_singles(df)
    df = calculate_projected_rbi(df)
    return df
