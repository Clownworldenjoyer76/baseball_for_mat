# projection_formulas.py
# projection_formulas.py
import pandas as pd
from utils import safe_col

# ─────────────────────────────
# 📌 BATTER PROJECTIONS
# ─────────────────────────────

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in [
        "adj_woba_combined", "whiff_percent", "zone_swing_miss_percent",
        "out_of_zone_swing_miss_percent", "gb_percent", "fb_percent",
        "innings_pitched", "strikeouts", "park_factor", "weather_factor"
    ]:
        df[col] = safe_col(df, col, 0)
    df["ip_per_start"] = df["innings_pitched"] / 20.0
    df["k_per_9"] = df["strikeouts"] / (df["innings_pitched"] / 9.0).replace(0, 1)
    base_score = (
        df["adj_woba_combined"] * 2.5 +
        df["fb_percent"] * 0.05 +
        df["gb_percent"] * -0.01 +
        df["whiff_percent"] * -0.02 +
        df["zone_swing_miss_percent"] * -0.015 +
        df["out_of_zone_swing_miss_percent"] * -0.015 +
        df["ip_per_start"] * -0.15 +
        df["k_per_9"] * -0.05
    )
    df["projected_total_bases"] = (base_score * df["park_factor"] * df["weather_factor"]).clip(lower=0).round(2)
    return df

def calculate_projected_hits(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["projected_hits"] = (
        safe_col(df, "hit", 0) * 0.6 +
        safe_col(df, "babip", 0) * 0.5 -
        (safe_col(df, "walks", 0) / safe_col(df, "innings_pitched", 1)) * 0.5
    ) * safe_col(df, "park_factor", 1) * safe_col(df, "weather_factor", 1)
    df["projected_hits"] = df["projected_hits"].clip(lower=0).round(2)
    return df

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    pitcher_walk_rate = safe_col(df, "walks", 0) / safe_col(df, "innings_pitched", 1)
    df["projected_walks"] = (
        safe_col(df, "bb_percent", 0) * 0.75 -
        pitcher_walk_rate * 0.25
    ) * safe_col(df, "weather_factor", 1)
    df["projected_walks"] = df["projected_walks"].clip(lower=0).round(2)
    return df

def calculate_projected_singles(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["projected_singles"] = (
        safe_col(df, "projected_hits", 0) -
        safe_col(df, "double", 0) -
        safe_col(df, "triple", 0) -
        safe_col(df, "home_run", 0)
    ).clip(lower=0).round(2)
    return df

def calculate_projected_rbi(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["projected_rbi"] = (
        safe_col(df, "home_run", 0) * 1.2 +
        safe_col(df, "hit", 0) * 0.3 +
        safe_col(df, "b_rbi", 0) * 0.6
    ) * safe_col(df, "park_factor", 1)
    df["projected_rbi"] = df["projected_rbi"].clip(lower=0).round(2)
    return df

def calculate_projected_home_runs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hr_9"] = safe_col(df, "home_run", 0) / (safe_col(df, "innings_pitched", 1) / 9.0)
    df["projected_home_runs"] = (
        safe_col(df, "home_run", 0) * 0.6 +
        safe_col(df, "fb_percent", 0) * 0.05 -
        df["hr_9"] * 0.4
    ) * safe_col(df, "park_factor", 1) * safe_col(df, "weather_factor", 1)
    df["projected_home_runs"] = df["projected_home_runs"].clip(lower=0).round(2)
    return df

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_projected_total_bases(df)
    df = calculate_projected_hits(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_singles(df)
    df = calculate_projected_rbi(df)
    df = calculate_projected_home_runs(df)
    return df

# ─────────────────────────────
# 📌 PITCHER PROJECTIONS
# ─────────────────────────────

def calculate_pitcher_strikeouts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["projected_strikeouts"] = (
        safe_col(df, "strikeouts", 0) / safe_col(df, "innings_pitched", 1) * 5.5
    ) * safe_col(df, "weather_factor", 1)
    df["projected_strikeouts"] = df["projected_strikeouts"].clip(lower=0).round(2)
    return df

def calculate_pitcher_walks(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    walk_rate = safe_col(df, "walks", 0) / safe_col(df, "innings_pitched", 1)
    df["projected_walks_allowed"] = (
        walk_rate * 5.5
    ) * safe_col(df, "weather_factor", 1)
    df["projected_walks_allowed"] = df["projected_walks_allowed"].clip(lower=0).round(2)
    return df

def calculate_pitcher_home_runs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    hr_9 = safe_col(df, "home_run", 0) / (safe_col(df, "innings_pitched", 1) / 9.0)
    df["projected_home_runs_allowed"] = (
        hr_9 * (5.5 / 9.0)
    ) * safe_col(df, "park_factor", 1) * safe_col(df, "weather_factor", 1)
    df["projected_home_runs_allowed"] = df["projected_home_runs_allowed"].clip(lower=0).round(2)
    return df

def calculate_pitcher_ip(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["projected_innings_pitched"] = (
        safe_col(df, "innings_pitched", 0) / 20.0 * 5.5
    ).clip(lower=0).round(2)
    return df

def calculate_all_pitcher_projections(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_pitcher_ip(df)
    df = calculate_pitcher_strikeouts(df)
    df = calculate_pitcher_walks(df)
    df = calculate_pitcher_home_runs(df)
    return df

# ─────────────────────────────
# 📌 FINAL SCORE PROJECTION
# ─────────────────────────────

def project_final_score(batter_df: pd.DataFrame) -> pd.DataFrame:
    df = batter_df.copy()

    # Expecting columns: team, team_type (home/away), projected_runs, park_factor, weather_factor
    df["adjusted_runs"] = (
        safe_col(df, "projected_runs", 0) *
        safe_col(df, "park_factor", 1) *
        safe_col(df, "weather_factor", 1)
    )

    team_scores = (
        df.groupby(["team", "team_type"])
          .agg(avg_proj_runs=("adjusted_runs", "mean"))
          .reset_index()
    )
    team_scores["team_score"] = (team_scores["avg_proj_runs"] * 9).round(2)

    home = team_scores.loc[team_scores["team_type"] == "home"]
    away = team_scores.loc[team_scores["team_type"] == "away"]

    if home.empty or away.empty:
        return pd.DataFrame([{
            "home_team": home["team"].iloc[0] if not home.empty else None,
            "away_team": away["team"].iloc[0] if not away.empty else None,
            "home_score": home["team_score"].iloc[0] if not home.empty else 0,
            "away_score": away["team_score"].iloc[0] if not away.empty else 0,
            "total_score": 0
        }])

    return pd.DataFrame([{
        "home_team": home["team"].iloc[0],
        "away_team": away["team"].iloc[0],
        "home_score": home["team_score"].iloc[0],
        "away_score": away["team_score"].iloc[0],
        "total_score": round(home["team_score"].iloc[0] + away["team_score"].iloc[0], 2)
    }])
