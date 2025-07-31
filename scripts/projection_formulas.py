
import pandas as pd
from utils import safe_col

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in [
        "hit", "home_run", "rbi", "bb_percent", "obp", "slg_percent", "woba",
        "era", "xfip", "whip", "k_percent", "bb_percent_pitcher"
    ]:
        df[col] = safe_col(df, col, 0)

    df["estimated_pa"] = 4.1
    df["estimated_ab"] = 3.6

    # Adjusted: reduce denominator to 400
    df["hit_rate"] = df["hit"] / 400
    df["hr_rate"] = df["home_run"] / 400
    df["slg_rate"] = df["slg_percent"]
    df["woba_rate"] = df["woba"]

    df["k_mult"] = (1 - df["k_percent"] / 100).clip(lower=0.7)
    df["whip_mult"] = (1 - df["whip"] / 10).clip(lower=0.75)
    df["era_mult"] = (1 - df["era"] / 10).clip(lower=0.70)
    df["xfip_mult"] = (1 - df["xfip"] / 10).clip(lower=0.75)

    df["hit_rate_adj"] = df["hit_rate"] * df["k_mult"] * df["whip_mult"]
    df["hr_rate_adj"] = df["hr_rate"] * df["xfip_mult"]
    df["slg_rate_adj"] = df["slg_rate"] * df["era_mult"]
    df["woba_rate_adj"] = df["woba_rate"] * df["era_mult"] * df["whip_mult"]

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
