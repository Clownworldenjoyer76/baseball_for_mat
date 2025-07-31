
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

    # Estimated plate appearances and ABs per game
    df["estimated_pa"] = 4.1
    df["estimated_ab"] = 3.6

    # Normalize batter season stats to per-PA rates
    df["hit_rate"] = df["hit"] / 600  # assume 600 PA season
    df["hr_rate"] = df["hr"] / 600
    df["slg_rate"] = df["slg"]  # already a rate
    df["woba_rate"] = df["woba"]  # already a rate

    # Adjusted rates with pitcher suppression
    df["hit_rate_adj"] = df["hit_rate"] * (1 - df["k_percent"] / 100) * (1 - df["whip"] / 10)
    df["hr_rate_adj"] = df["hr_rate"] * (1 - df["xfip"] / 10)
    df["slg_rate_adj"] = df["slg_rate"] * (1 - df["era"] / 10)
    df["woba_rate_adj"] = df["woba_rate"] * (1 - df["era"] / 10) * (1 - df["whip"] / 10)

    # Project per-game outputs
    df["total_hits_projection"] = df["hit_rate_adj"] * df["estimated_pa"]
    df["avg_hr"] = df["hr_rate_adj"] * df["estimated_pa"]
    df["total_bases_projection"] = df["slg_rate_adj"] * df["estimated_ab"]
    df["avg_woba"] = df["woba_rate_adj"]

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
