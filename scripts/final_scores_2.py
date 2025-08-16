#!/usr/bin/env python3
"""
Build per-game projected runs (using only the two provided inputs) and write to:
  Output: data/bets/game_props_history.csv

Inputs:
  - data/bets/prep/batter_props_final.csv
  - data/bets/prep/pitcher_props_bets.csv

Leaves these columns BLANK in the output:
  favorite_correct, actual_real_run_total, run_total_diff, home_score, away_score
"""

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# ---------------- Configuration (tunable but fixed here) ----------------
INPUT_BATTERS = Path("data/bets/prep/batter_props_final.csv")
INPUT_PITCHERS = Path("data/bets/prep/pitcher_props_bets.csv")
OUTPUT_FILE   = Path("data/bets/game_props_history.csv")

# League-level baseline & weights (simple, explainable; tune later)
BASELINE_RUNS_PER_TEAM = 4.5
ALPHA_OFFENSE = 0.8    # strength of lineup z on runs
BETA_PITCHING = 0.8    # strength of opposing SP z on runs

# If over_probability is missing/noisy, clamp to this range before weighting
PROB_MIN, PROB_MAX = 0.50, 0.99

# ---------------- Helper functions ----------------
def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    # Normalize common key fields if present
    for c in ("team", "opp_team", "player", "name"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    # Standardize date if present
    if "date" in df.columns:
        # Keep as string for matching the existing CSV schema
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
    return df

def safe_mean(x: pd.Series) -> float:
    if len(x) == 0:
        return np.nan
    return float(np.nanmean(x.values))

def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = values.astype(float)
    w = weights.astype(float)
    mask = np.isfinite(v) & np.isfinite(w)
    if not mask.any():
        return np.nan
    return float(np.average(v[mask], weights=w[mask]))

def pick_two_teams(team_series: pd.Series) -> tuple[str, str]:
    teams = sorted(set([t for t in team_series if isinstance(t, str) and t]))
    if len(teams) == 2:
        return teams[0], teams[1]
    # Fallback: best effort
    if len(teams) == 1:
        return teams[0], teams[0]
    return "", ""

# ---------------- Load inputs ----------------
if not INPUT_BATTERS.exists():
    sys.exit(f"ERROR: Missing input file: {INPUT_BATTERS}")
if not INPUT_PITCHERS.exists():
    sys.exit(f"ERROR: Missing input file: {INPUT_PITCHERS}")

bat = _prep(pd.read_csv(INPUT_BATTERS))
pit = _prep(pd.read_csv(INPUT_PITCHERS))

# Columns we hope/assume exist in batter file (defensive checks below)
# - team, opp_team, batter_z or mega_z, over_probability, opp_pitcher_z, game_id, date
# Pitcher file is optional for this calculation (batters already carry opp_pitcher_z),
# but we load it to potentially future-proof or validate.

# ---------------- Build team-game aggregates from batter props ----------------
# Choose batter strength metric (prefer mega_z if present; else batter_z; else 0)
if "mega_z" in bat.columns:
    bat_strength_col = "mega_z"
elif "batter_z" in bat.columns:
    bat_strength_col = "batter_z"
else:
    # If neither present, create a zero column
    bat_strength_col = "_tmp_bat_strength"
    bat[bat_strength_col] = 0.0

# Build weight vector from over_probability if available
if "over_probability" in bat.columns:
    weights = bat["over_probability"].clip(PROB_MIN, PROB_MAX)
else:
    weights = pd.Series(1.0, index=bat.index)

# Opponent SP z-score (prefer opp_pitcher_z if present)
opp_sp_col = "opp_pitcher_z" if "opp_pitcher_z" in bat.columns else None

group_keys = [k for k in ["date", "game_id", "team", "opp_team"] if k in bat.columns]
if not group_keys:
    # Fall back to pairing by (team, opp_team) only
    for k in ["team", "opp_team"]:
        if k not in bat.columns:
            bat[k] = ""
    group_keys = ["team", "opp_team"]

agg_rows = []
for keys, df in bat.groupby(group_keys, dropna=False):
    # Normalize keys to dict
    if not isinstance(keys, tuple):
        keys = (keys,)
    keydict = dict(zip(group_keys, keys))

    # Offense strength = weighted mean of batter z
    off_strength = weighted_mean(df[bat_strength_col], weights.loc[df.index])

    # Opponent SP z (same within matchup rows; take mean to be safe)
    opp_sp_z = safe_mean(df[opp_sp_col]) if opp_sp_col and opp_sp_col in df.columns else np.nan

    agg_rows.append({
        **keydict,
        "offense_strength_z": off_strength,
        "opp_sp_strength_z": opp_sp_z,
    })

team_game = pd.DataFrame(agg_rows)

# Ensure date/game_id columns exist (string for date)
if "date" not in team_game.columns:
    team_game["date"] = pd.NA
if "game_id" not in team_game.columns:
    # Create a synthetic game_id from team/opp_team if needed
    team_game["game_id"] = (
        team_game.get("team", "").astype(str).str[:3] + "_" +
        team_game.get("opp_team", "").astype(str).str[:3]
    )

# ---------------- Pair into single game rows and compute μ ----------------
# Some rows are "team vs opp_team". We want one row per game with both sides' μ.
# We'll merge each pair by using a canonical key: frozenset({team, opp_team}) + date (+ game_id if present).
def canon_key(row: pd.Series) -> tuple:
    a = str(row.get("team", "")).strip()
    b = str(row.get("opp_team", "")).strip()
    d = str(row.get("date", ""))
    gid = str(row.get("game_id", ""))
    pair = tuple(sorted([a, b]))
    # prefer (date, game_id, pair) to reduce accidental collisions
    return (d, gid, pair[0], pair[1])

team_game["_ckey"] = team_game.apply(canon_key, axis=1)

games = []
for _, grp in team_game.groupby("_ckey"):
    t1, t2 = pick_two_teams(pd.concat([grp["team"], grp["opp_team"]], ignore_index=True))
    if not t1 or not t2:
        continue

    # Extract strengths for each side
    # side A row: team == t1
    a_row = grp.loc[grp["team"] == t1].head(1)
    b_row = grp.loc[grp["team"] == t2].head(1)

    # In case structure is flipped (team/opp), attempt mirror
    if a_row.empty and not grp.empty:
        a_row = grp.iloc[[0]]
    if b_row.empty and len(grp) > 1:
        b_row = grp.iloc[[1]]

    # Pull keys
    date_val = str(a_row["date"].iloc[0]) if "date" in a_row.columns else str(pd.NA)
    gid_val  = str(a_row["game_id"].iloc[0]) if "game_id" in a_row.columns else ""

    # Offense strengths
    a_off = float(a_row["offense_strength_z"].iloc[0]) if not a_row.empty else 0.0
    b_off = float(b_row["offense_strength_z"].iloc[0]) if not b_row.empty else 0.0

    # Opposing SP z for each side (from batter rows)
    a_opp_sp = float(a_row["opp_sp_strength_z"].iloc[0]) if not a_row.empty else 0.0
    b_opp_sp = float(b_row["opp_sp_strength_z"].iloc[0]) if not b_row.empty else 0.0

    # Projected means (μ) using the simple linear log-less mapping:
    # μ_team = baseline + α * offense_strength_z - β * opp_sp_strength_z
    a_mu = BASELINE_RUNS_PER_TEAM + ALPHA_OFFENSE * a_off - BETA_PITCHING * a_opp_sp
    b_mu = BASELINE_RUNS_PER_TEAM + ALPHA_OFFENSE * b_off - BETA_PITCHING * b_opp_sp

    # Hard clip to non-negative
    a_mu = max(0.0, float(a_mu))
    b_mu = max(0.0, float(b_mu))

    games.append({
        "date": date_val,
        "game_id": gid_val,
        "home_team": None,  # will be set later if template provides; else alphabetical
        "away_team": None,
        "team_a": t1,
        "team_b": t2,
        "proj_team_a_runs": round(a_mu, 3),
        "proj_team_b_runs": round(b_mu, 3),
        "proj_total": round(a_mu + b_mu, 3),
    })

proj_df = pd.DataFrame(games)

# If nothing to write, still create an empty output with required blank columns
if proj_df.empty:
    proj_df = pd.DataFrame(columns=[
        "date", "game_id", "home_team", "away_team",
        "proj_team_a_runs", "proj_team_b_runs", "proj_total"
    ])

# ---------------- Integrate with existing output schema (if any) ----------------
existing_cols = []
template = None
if OUTPUT_FILE.exists():
    try:
        template = pd.read_csv(OUTPUT_FILE)
        template = _prep(template)
        existing_cols = template.columns.tolist()
    except Exception:
        template = None

# Decide home/away:
# 1) If template has home_team/away_team, align μ to those team names when we can match.
# 2) Else, set alphabetically: home = min(team_a, team_b), away = max(team_a, team_b).
def align_home_away(row: pd.Series, home: str|None, away: str|None) -> dict:
    t1, t2 = str(row["team_a"]), str(row["team_b"])
    mu_a, mu_b = float(row["proj_team_a_runs"]), float(row["proj_team_b_runs"])
    if home and away and home in (t1, t2) and away in (t1, t2):
        if home == t1:
            return {"home_team": t1, "away_team": t2, "proj_home_runs": mu_a, "proj_away_runs": mu_b}
        else:
            return {"home_team": t2, "away_team": t1, "proj_home_runs": mu_b, "proj_away_runs": mu_a}
    # fallback alphabetical
    h, a = sorted([t1, t2])
    if h == t1:
        return {"home_team": h, "away_team": a, "proj_home_runs": mu_a, "proj_away_runs": mu_b}
    else:
        return {"home_team": h, "away_team": a, "proj_home_runs": mu_b, "proj_away_runs": mu_a}

# Build final frame
final_rows = []
if template is not None and not template.empty:
    # Try to map each projected game into the template by (date, game_id) primarily, else by team pair.
    tmpl_key_cols = [c for c in ["date", "game_id", "home_team", "away_team"] if c in template.columns]

    # Create lookup from template for home/away resolution
    tmpl_lookup = {}
    for _, r in template.iterrows():
        key = (str(r.get("date", "")), str(r.get("game_id", "")),
               str(r.get("home_team", "")), str(r.get("away_team", "")))
        tmpl_lookup[key] = {"home_team": r.get("home_team", None), "away_team": r.get("away_team", None)}

    for _, r in proj_df.iterrows():
        # Prefer exact (date, game_id) match in template to extract declared home/away
        home_hint, away_hint = None, None
        k1 = (str(r.get("date", "")), str(r.get("game_id", "")),
              str(r.get("team_a", "")), str(r.get("team_b", "")))
        k2 = (str(r.get("date", "")), str(r.get("game_id", "")),
              str(r.get("team_b", "")), str(r.get("team_a", "")))

        # Find any template row with same date/game_id (regardless of teams)
        hits = [k for k in tmpl_lookup.keys() if k[0] == k1[0] and k[1] == k1[1]]
        if hits:
            home_hint = tmpl_lookup[hits[0]].get("home_team")
            away_hint = tmpl_lookup[hits[0]].get("away_team")

        aligned = align_home_away(r, home_hint, away_hint)

        out = {
            "date": r.get("date", pd.NA),
            "game_id": r.get("game_id", ""),
            "home_team": aligned["home_team"],
            "away_team": aligned["away_team"],
            "proj_home_runs": round(aligned["proj_home_runs"], 3),
            "proj_away_runs": round(aligned["proj_away_runs"], 3),
            "proj_total": round(r.get("proj_total", np.nan), 3),
        }
        final_rows.append(out)

    out_df = pd.DataFrame(final_rows)

    # Merge back into the template on (date, home_team, away_team, game_id) when present
    merge_keys = [c for c in ["date", "game_id", "home_team", "away_team"] if c in template.columns and c in out_df.columns]
    if not merge_keys:
        merge_keys = [c for c in ["date", "home_team", "away_team"] if c in template.columns and c in out_df.columns]

    merged = template.merge(out_df, on=merge_keys, how="left", suffixes=("", "_proj"))

    # If template already has proj_* columns, update them; else keep the *_proj we just added
    for col in ["proj_home_runs", "proj_away_runs", "proj_total"]:
        if col in merged.columns and f"{col}_proj" in merged.columns:
            merged[col] = merged[f"{col}_proj"].combine_first(merged[col])
            merged.drop(columns=[f"{col}_proj"], inplace=True, errors="ignore")
        elif f"{col}_proj" in merged.columns:
            merged.rename(columns={f"{col}_proj": col}, inplace=True)

    final = merged

else:
    # No template: construct a clean output with standard columns
    aligned = proj_df.apply(lambda r: align_home_away(r, None, None), axis=1, result_type="expand")
    final = pd.DataFrame({
        "date": proj_df["date"],
        "game_id": proj_df["game_id"],
        "home_team": aligned["home_team"],
        "away_team": aligned["away_team"],
        "proj_home_runs": aligned["proj_home_runs"].round(3),
        "proj_away_runs": aligned["proj_away_runs"].round(3),
        "proj_total": proj_df["proj_total"].round(3),
    })

# ---------------- Ensure required blank columns ----------------
for blank_col in ["favorite_correct", "actual_real_run_total", "run_total_diff", "home_score", "away_score"]:
    if blank_col not in final.columns:
        final[blank_col] = pd.NA
    else:
        final[blank_col] = pd.NA

# Optional: stable column order if we created from scratch
preferred_order = [
    "date", "game_id", "home_team", "away_team",
    "proj_home_runs", "proj_away_runs", "proj_total",
    "home_score", "away_score", "actual_real_run_total",
    "run_total_diff", "favorite_correct"
]
# Keep existing columns first (if any), then append missing preferred columns in order
ordered = []
seen = set()
for c in final.columns:
    if c not in seen:
        ordered.append(c); seen.add(c)
for c in preferred_order:
    if c not in seen and c in final.columns:
        ordered.append(c); seen.add(c)

final = final[ordered]

# ---------------- Write output ----------------
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final.to_csv(OUTPUT_FILE, index=False)

print(f"Wrote {len(final)} rows to {OUTPUT_FILE}")
