#Projected Pitcher Props 7.30.25
import pandas as pd
from pathlib import Path
from utils import safe_col

# Output path
OUTPUT_PATH = Path("data/_projections/pitcher_projections.csv")

def calculate_projected_strikeouts(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["k_percent"] = safe_col(df_copy, "k_percent", 0) / 100
    df_copy["opponent_k_rate"] = safe_col(df_copy, "opponent_k_rate", 0.22)
    df_copy["projected_bf"] = safe_col(df_copy, "projected_bf", 22)

    df_copy["projected_strikeouts"] = (
        df_copy["k_percent"] * df_copy["opponent_k_rate"] * df_copy["projected_bf"]
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["bb_percent"] = safe_col(df_copy, "bb_percent", 0) / 100
    df_copy["opponent_bb_rate"] = safe_col(df_copy, "opponent_bb_rate", 0.08)
    df_copy["projected_bf"] = safe_col(df_copy, "projected_bf", 22)

    df_copy["projected_walks"] = (
        df_copy["bb_percent"] * df_copy["opponent_bb_rate"] * df_copy["projected_bf"]
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_outs(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["projected_ip"] = safe_col(df_copy, "projected_ip", 5.1)

    df_copy["projected_outs"] = (df_copy["projected_ip"] * 3).clip(lower=0).round(2)
    return df_copy

def calculate_projected_hits_allowed(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["hits_per_9"] = safe_col(df_copy, "hits_per_9", 8.5)
    df_copy["projected_ip"] = safe_col(df_copy, "projected_ip", 5.1)

    df_copy["projected_hits_allowed"] = (
        df_copy["hits_per_9"] * df_copy["projected_ip"] / 9
    ).clip(lower=0).round(2)

    return df_copy

def calculate_projected_earned_runs(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy["era"] = safe_col(df_copy, "era", 4.2)
    df_copy["projected_ip"] = safe_col(df_copy, "projected_ip", 5.1)

    df_copy["projected_earned_runs"] = (
        df_copy["era"] * df_copy["projected_ip"] / 9
    ).clip(lower=0).round(2)

    return df_copy

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_projected_strikeouts(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_outs(df)
    df = calculate_projected_hits_allowed(df)
    df = calculate_projected_earned_runs(df)
    return df
