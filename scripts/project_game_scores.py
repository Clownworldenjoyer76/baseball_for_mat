# scripts/project_game_scores.py
#
# Combine batter, pitcher, park, and weather data to project runs, props, and game scores.

import pandas as pd
import numpy as np
from pathlib import Path

# === File Paths ===
DATA_DIR = Path("data/_projections")
FINAL_DIR = Path("data/end_chain/final")

batters_proj_file = DATA_DIR / "batter_props_projected_final.csv"
batters_exp_file = DATA_DIR / "batter_props_expanded_final.csv"
pitchers_proj_file = DATA_DIR / "pitcher_props_projected_final.csv"
pitchers_mega_file = DATA_DIR / "pitcher_mega_z_final.csv"

# === Load Data ===
batters_proj = pd.read_csv(batters_proj_file)
batters_exp = pd.read_csv(batters_exp_file)
pitchers_proj = pd.read_csv(pitchers_proj_file)
pitchers_mega = pd.read_csv(pitchers_mega_file)

# === Merge Batter + Pitcher + Park/Weather Context ===
batters = batters_proj.merge(
    batters_exp[["player_id", "game_id", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]],
    on=["player_id", "game_id"],
    how="left"
)

# Scale batter projections by park/weather factor
for col in ["prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5"]:
    batters[col] = (
        batters[col] *
        batters["adj_woba_combined"].fillna(1.0)
    )

# Join pitcher context by game
pitchers = pitchers_proj.merge(
    pitchers_mega[["player_id", "game_id", "k_percent_eff", "bb_percent_eff"]],
    on=["player_id", "game_id"],
    how="left"
)

# === Project Runs ===
def project_team_runs(batters_team, pitcher_opp):
    """
    Estimate expected runs for one team given lineup and opposing pitcher.
    """
    # Expected PA = sum of projected plate appearances
    pa_total = batters_team["proj_pa_used"].sum()

    # Adjust batter hit probability by opposing pitcher K% and BB%
    hit_prob = batters_team["proj_avg_used"].mean() * (1 - pitcher_opp["k_percent_eff"].mean())
    hr_prob = batters_team["proj_hr_rate_pa_used"].mean() * (1 - pitcher_opp["k_percent_eff"].mean())

    # Expected hits and HRs
    exp_hits = pa_total * hit_prob
    exp_hrs = pa_total * hr_prob

    # Translate to runs: HRs score directly, hits scale by park factor
    park_factor = batters_team["adj_woba_park"].mean(skipna=True) if "adj_woba_park" in batters_team else 1.0
    exp_runs = (exp_hits * 0.25 + exp_hrs) * park_factor

    return exp_runs

# === Aggregate by Game ===
results = []
for game_id in batters["game_id"].dropna().unique():
    bat_team = batters[batters["game_id"] == game_id]
    pit_team = pitchers[pitchers["game_id"] == game_id]

    if bat_team.empty or pit_team.empty:
        continue

    # Assume one main pitcher per team (starting pitcher)
    for team in bat_team["team"].unique():
        opp_pitchers = pit_team[pit_team["team_id"] != team]
        if opp_pitchers.empty:
            continue
        exp_runs = project_team_runs(
            batters_team=bat_team[bat_team["team"] == team],
            pitcher_opp=opp_pitchers
        )
        results.append({
            "game_id": game_id,
            "team": team,
            "expected_runs": exp_runs
        })

results_df = pd.DataFrame(results)

# === Save Outputs ===
out_file = FINAL_DIR / "game_score_projections.csv"
results_df.to_csv(out_file, index=False)

print(f"✅ Wrote {len(results_df)} rows -> {out_file}")
