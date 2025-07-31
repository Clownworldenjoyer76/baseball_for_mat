
import pandas as pd

def calculate_all_projections(df):
    # Safe conversions
    for col in ["hit", "home_run", "slg_percent", "woba", "era", "whip", "k_percent", "bb_percent"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Base projection rates
    df["hit_rate"] = df["hit"] / 400
    df["hr_rate"] = df["home_run"] / 400
    df["tb_rate"] = df["slg_percent"]
    df["woba_rate"] = df["woba"]

    # Modifier based on plate discipline
    df["discipline_factor"] = 1 - (df["k_percent"] + df.get("bb_percent", 0)) / 200

    # Apply suppression by ERA + WHIP (normalized)
    df["suppression_factor"] = 1 - ((df["era"] / 10) + (df["whip"] / 3)) / 2
    df["suppression_factor"] = df["suppression_factor"].clip(lower=0.25)

    # Final projections
    df["total_hits_projection"] = (df["hit_rate"] * df["suppression_factor"]).round(2)
    df["avg_hr"] = (df["hr_rate"] * df["suppression_factor"]).round(2)
    df["total_bases_projection"] = (df["tb_rate"] * df["suppression_factor"]).round(2)
    df["avg_woba"] = (df["woba_rate"] * df["suppression_factor"]).round(3)

    return df
