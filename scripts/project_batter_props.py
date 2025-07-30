
import pandas as pd
from utils import safe_col

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["xSLG"] = safe_col(df_copy, "xSLG", 0.400)
    df_copy["projected_AB"] = safe_col(df_copy, "projected_AB", 4)
    df_copy["park_tb_boost"] = safe_col(df_copy, "park_tb_boost", 0)
    df_copy["weather_tb_boost"] = safe_col(df_copy, "weather_tb_boost", 0)

    df_copy["projected_total_bases"] = (
        (df_copy["xSLG"] + df_copy["park_tb_boost"] + df_copy["weather_tb_boost"]) *
        df_copy["projected_AB"]
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_hits(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["xBA"] = safe_col(df_copy, "xBA", 0.250)
    df_copy["projected_AB"] = safe_col(df_copy, "projected_AB", 4)

    df_copy["projected_hits"] = (df_copy["xBA"] * df_copy["projected_AB"]).clip(lower=0).round(2)
    return df_copy

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["batter_bb_percent"] = safe_col(df_copy, "bb_percent", 0) / 100
    df_copy["pitcher_zone_percent"] = safe_col(df_copy, "pitcher_zone_percent", 50) / 100
    df_copy["projected_PA"] = safe_col(df_copy, "projected_PA", 4.2)

    df_copy["projected_walks"] = (
        df_copy["batter_bb_percent"] * (1 - df_copy["pitcher_zone_percent"]) * df_copy["projected_PA"]
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_singles(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["projected_hits"] = safe_col(df_copy, "projected_hits", 0)
    df_copy["double"] = safe_col(df_copy, "double", 0)
    df_copy["triple"] = safe_col(df_copy, "triple", 0)
    df_copy["home_run"] = safe_col(df_copy, "home_run", 0)

    df_copy["projected_singles"] = (
        df_copy["projected_hits"] -
        df_copy["double"] -
        df_copy["triple"] -
        df_copy["home_run"]
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_rbi(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["home_run"] = safe_col(df_copy, "home_run", 0)
    df_copy["projected_hits"] = safe_col(df_copy, "projected_hits", 0)
    df_copy["lineup_rbi_factor"] = safe_col(df_copy, "lineup_rbi_factor", 1.0)
    df_copy["team_runs_projected"] = safe_col(df_copy, "team_runs_projected", 4.5)

    df_copy["projected_rbi"] = (
        df_copy["home_run"] * 1.3 +
        df_copy["projected_hits"] * 0.3 +
        df_copy["lineup_rbi_factor"] * df_copy["team_runs_projected"] * 0.1
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_home_runs(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["hr_fb"] = safe_col(df_copy, "hr_fb", 0.12)
    df_copy["fb_percent"] = safe_col(df_copy, "fb_percent", 0.3)
    df_copy["projected_bbe"] = safe_col(df_copy, "projected_bbe", 3)

    df_copy["projected_home_runs"] = (
        df_copy["hr_fb"] * df_copy["fb_percent"] * df_copy["projected_bbe"]
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
