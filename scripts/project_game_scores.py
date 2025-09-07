#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/project_game_scores.py
#
# Combine batter and pitcher projections with park/weather adjustments
# to produce expected team runs per game_id.

from pathlib import Path
import pandas as pd

# === Paths ===
DATA_DIR = Path("data/_projections")
FINAL_DIR = Path("data/end_chain/final")
FINAL_DIR.mkdir(parents=True, exist_ok=True)

batters_proj_file  = DATA_DIR / "batter_props_projected_final.csv"
batters_exp_file   = DATA_DIR / "batter_props_expanded_final.csv"
pitchers_proj_file = DATA_DIR / "pitcher_props_projected_final.csv"

# === Load ===
batters_proj  = pd.read_csv(batters_proj_file)
batters_exp   = pd.read_csv(batters_exp_file)
pitchers_proj = pd.read_csv(pitchers_proj_file)

# Enforce numeric IDs where present
for df in (batters_proj, batters_exp, pitchers_proj):
    for col in ("player_id", "game_id", "team_id", "opponent_team_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

# === Merge Batter + Adjustments ===
adj_cols = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

left_cols = ["player_id", "game_id"] + [c for c in adj_cols if c in batters_exp.columns]
batters = batters_proj.merge(
    batters_exp[left_cols],
    on=["player_id", "game_id"],
    how="left",
    suffixes=("", "_exp")
)

# Ensure adj columns exist even if absent in both inputs
for c in adj_cols:
    if c not in batters.columns:
        batters[c] = pd.NA
    # coerce numeric
    batters[c] = pd.to_numeric(batters[c], errors="coerce")

# Defaults and combined fallback
batters["adj_woba_weather"] = batters["adj_woba_weather"].fillna(1.0)
batters["adj_woba_park"]    = batters["adj_woba_park"].fillna(1.0)

if "adj_woba_combined" not in batters.columns:
    batters["adj_woba_combined"] = pd.NA
batters["adj_woba_combined"] = batters["adj_woba_combined"].fillna(
    (batters["adj_woba_weather"] + batters["adj_woba_park"]) / 2.0
)

# === Scale batter prop probabilities by combined adjustment (clip to [0,1]) ===
for col in ["prob_hits_over_1p5", "prob_tb_over_1p5", "prob_hr_over_0p5"]:
    if col in batters.columns:
        batters[col] = (pd.to_numeric(batters[col], errors="coerce").fillna(0.0)
                        * batters["adj_woba_combined"]).clip(0.0, 1.0)

# === Pitcher context (k%/bb% already in props) ===
for c in ("k_percent_eff", "bb_percent_eff"):
    if c not in pitchers_proj.columns:
        pitchers_proj[c] = 0.0
    pitchers_proj[c] = pd.to_numeric(pitchers_proj[c], errors="coerce").fillna(0.0)

# === Team run projection ===
def project_team_runs(batters_team: pd.DataFrame, opp_pitchers: pd.DataFrame) -> float:
    pa_total = pd.to_numeric(batters_team.get("proj_pa_used", 0), errors="coerce").fillna(0).sum()
    k_eff = float(opp_pitchers["k_percent_eff"].mean()) if not opp_pitchers.empty else 0.0
    k_eff = max(0.0, min(1.0, k_eff))

    avg_used = pd.to_numeric(batters_team.get("proj_avg_used", 0), errors="coerce").fillna(0).mean()
    hr_pa    = pd.to_numeric(batters_team.get("proj_hr_rate_pa_used", 0), errors="coerce").fillna(0).mean()

    hit_prob = max(0.0, min(1.0, avg_used * (1.0 - k_eff)))
    hr_prob  = max(0.0, min(1.0, hr_pa    * (1.0 - k_eff)))

    exp_hits = pa_total * hit_prob
    exp_hrs  = pa_total * hr_prob

    park_factor = pd.to_numeric(batters_team.get("adj_woba_park", 1.0), errors="coerce").fillna(1.0).mean()
    exp_runs = (exp_hits * 0.25 + exp_hrs) * park_factor
    return float(exp_runs)

# === Aggregate by (game_id, team_id) ===
required_cols = {"game_id", "team_id", "team"}
missing_req = sorted(required_cols - set(batters.columns))
if missing_req:
    raise RuntimeError(f"Missing required columns in batters dataset: {missing_req}")

results = []
for (game_id, team_id), grp in batters.groupby(["game_id", "team_id"], dropna=True):
    opp_pitchers = pitchers_proj[
        (pitchers_proj["game_id"] == game_id) &
        (pd.to_numeric(pitchers_proj["opponent_team_id"], errors="coerce") == team_id)
    ]
    if opp_pitchers.empty or grp.empty:
        continue

    team_name = grp["team"].iloc[0]
    exp_runs = project_team_runs(grp, opp_pitchers)
    results.append({
        "game_id": int(game_id) if pd.notna(game_id) else game_id,
        "team_id": int(team_id) if pd.notna(team_id) else team_id,
        "team": team_name,
        "expected_runs": exp_runs
    })

results_df = pd.DataFrame(results).sort_values(["game_id", "team_id"]).reset_index(drop=True)

# === Output ===
out_file = FINAL_DIR / "game_score_projections.csv"
results_df.to_csv(out_file, index=False)
print(f"OK {len(results_df)} rows -> {out_file}")
