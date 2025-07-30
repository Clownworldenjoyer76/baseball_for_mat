import pandas as pd
from pathlib import Path

# === INPUT FILES ===
BAT_HOME = Path("data/end_chain/final/batter_home_final.csv")
BAT_AWAY = Path("data/end_chain/final/batter_away_final.csv")
PITCHERS = Path("data/end_chain/final/startingpitchers_final.csv")
PARK_FACTORS = Path("weather_input.csv")
WEATHER = Path("data/weather_adjustments.csv")

# === OUTPUT FILE ===
OUTPUT = Path("data/_projections/final_scores.csv")

# === LOAD DATA ===
b_home = pd.read_csv(BAT_HOME)
b_away = pd.read_csv(BAT_AWAY)
p = pd.read_csv(PITCHERS)
parks = pd.read_csv(PARK_FACTORS)
weather = pd.read_csv(WEATHER)

# === CLEAN TEAM NAMES ===
for df in [b_home, b_away, p, parks, weather]:
    if "team" in df.columns:
        df["team"] = df["team"].astype(str).str.strip()

# === ENVIRONMENT BOOSTS ===
parks = parks[["team", "run_factor"]].copy()
parks["park_run_boost"] = parks["run_factor"] - 1

def calc_weather_boost(row):
    temp_boost = (row["temperature"] - 70) * 0.005
    wind_boost = row["wind_speed"] * 0.003
    return round(temp_boost + wind_boost, 4)

weather["weather_run_boost"] = weather.apply(calc_weather_boost, axis=1)
env = pd.merge(parks, weather, on="team", how="outer")
env["env_multiplier"] = 1 + env["park_run_boost"] + env["weather_run_boost"]

# === PITCHER WEAKNESS ===
p["pitcher_weakness"] = (
    1 + ((p["projected_earned_runs"] - 4.2) / 4.2).clip(lower=-0.3, upper=0.5)
).round(4)

# === TEAM OFFENSE SCORE ===
def compute_offense_score(df):
    df["projected_rbi"] = df.get("projected_rbi", 0)
    df["projected_total_bases"] = df.get("projected_total_bases", 0)
    df["projected_walks"] = df.get("projected_walks", 0)
    return (
        df.groupby("team").apply(
            lambda g: (
                g["projected_rbi"].sum() +
                0.1 * g["projected_total_bases"].sum() +
                0.3 * g["projected_walks"].sum()
            )
        ).rename("team_offense_score")
    )

home_offense = compute_offense_score(b_home)
away_offense = compute_offense_score(b_away)

# === COMBINE EVERYTHING TO FINAL SCORES ===
home_team = b_home["team"].unique()[0]
away_team = b_away["team"].unique()[0]

home_pitcher = p[p["team"] == home_team]
away_pitcher = p[p["team"] == away_team]

home_env = env[env["team"] == home_team]
away_env = env[env["team"] == away_team]

df_out = pd.DataFrame([
    {
        "team": home_team,
        "opponent": away_team,
        "projected_runs": round(
            home_offense.loc[home_team] *
            away_pitcher["pitcher_weakness"].values[0] *
            home_env["env_multiplier"].values[0], 2
        )
    },
    {
        "team": away_team,
        "opponent": home_team,
        "projected_runs": round(
            away_offense.loc[away_team] *
            home_pitcher["pitcher_weakness"].values[0] *
            away_env["env_multiplier"].values[0], 2
        )
    }
])

df_out.to_csv(OUTPUT, index=False)
