
import pandas as pd
from utils import safe_col

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure required columns exist
    for col in [
        "hit", "hr", "rbi", "bb_percent", "obp", "slg", "woba",
        "era", "xfip", "whip", "k_percent", "bb_percent_pitcher"
    ]:
        df[col] = safe_col(df, col, 0)

    # Estimated plate appearances per game
    df["estimated_pa"] = 4.1

    # Projected per-game hits vs specific pitcher
    df["total_hits_projection"] = (
        df["hit"] / df["estimated_pa"] * (1 - df["k_percent"] / 100)
        * (1 - df["whip"] / 10)
        * df["estimated_pa"]
    ).round(2)

    # Projected per-game total bases
    df["total_bases_projection"] = (
        df["slg"] * (1 - df["era"] / 10)
        * df["estimated_pa"] / 4.0
    ).round(2)

    # Projected per-game home runs
    df["avg_hr"] = (
        df["hr"] / df["estimated_pa"]
        * (1 - df["xfip"] / 10)
        * df["estimated_pa"]
    ).round(2)

    # Projected per-game wOBA adjusted by pitcher effectiveness
    df["avg_woba"] = (
        df["woba"] * (1 - df["era"] / 10) * (1 - df["whip"] / 10)
    ).round(3)

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
