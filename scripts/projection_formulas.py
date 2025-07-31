
import pandas as pd
from utils import safe_col

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure required columns exist with fallback defaults
    for col in [
        "hit", "hr", "rbi", "bb_percent", "obp", "slg", "woba",
        "era", "xfip", "whip", "k_percent", "bb_percent_pitcher"
    ]:
        df[col] = safe_col(df, col, 0)

    # Projected total bases: batter + pitcher suppression
    df["total_bases_projection"] = (
        df["hit"] * 0.75 +
        df["hr"] * 1.4 +
        df["slg"] * 2.0 -
        df["whip"] * 0.8 -
        df["k_percent"] * 0.4
    ).round(2)

    # Projected hits: batter + pitcher interaction
    df["total_hits_projection"] = (
        df["hit"] * 0.6 +
        df["obp"] * 1.1 -
        df["era"] * 0.5 -
        df["xfip"] * 0.5
    ).round(2)

    # Add avg_hr and avg_woba for scoring model
    df["avg_hr"] = df["hr"]
    df["avg_woba"] = df["woba"]

    return df

def project_final_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in ["avg_woba", "avg_hr", "total_hits_projection", "total_bases_projection"]:
        df[col] = safe_col(df, col, 0)

    df["projected_final_score"] = (
        df["avg_woba"] * 5.25 +
        df["avg_hr"] * 1.25 +
        df["total_hits_projection"] * 0.65 +
        df["total_bases_projection"] * 0.45
    ).round(2)

    return df
