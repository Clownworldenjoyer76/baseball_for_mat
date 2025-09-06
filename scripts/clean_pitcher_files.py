#!/usr/bin/env python3
# scripts/clean_pitcher_files.py
#
# Cleans pitcher outputs.
# - data/_projections/pitcher_props_projected_final.csv:
#     * delete *_ctx columns
#     * set opponent_team_id by deriving from data/raw/todaysgames_normalized.csv
#       (team_id vs home_team_id/away_team_id → opponent = the other team_id)
#       If multiple distinct opponents for a team_id on the schedule, leave blank.
#     * coerce opponent_team_id to nullable integer (Int64)
# - data/_projections/pitcher_mega_z_final.csv:
#     * set role = "pitcher" for all rows
#     * inject/overwrite team_id from data/raw/lineups.csv by matching player_id
#       (keep first team_id per player_id in lineups)
#
# All writes are in-place.

from pathlib import Path
import pandas as pd
import sys

# ---- Paths ----
PROPS_FILE = Path("data/_projections/pitcher_props_projected_final.csv")
MEGA_FILE  = Path("data/_projections/pitcher_mega_z_final.csv")
SCHEDULE_FILE = Path("data/raw/todaysgames_normalized.csv")
LINEUPS_FILE  = Path("data/raw/lineups.csv")

# Columns to remove from PROPS_FILE
CTX_COLS = [
    "game_id_ctx", "team_id_ctx", "park_factor_ctx", "role_ctx",
    "city_ctx", "state_ctx", "timezone_ctx", "is_dome_ctx",
]

def die(msg: str) -> None:
    sys.stderr.write(f"ERROR: {msg}\n")
    sys.exit(1)

def clean_props():
    if not PROPS_FILE.exists():
        die(f"Missing file: {PROPS_FILE}")
    if not SCHEDULE_FILE.exists():
        die(f"Missing file: {SCHEDULE_FILE}")

    df = pd.read_csv(PROPS_FILE)

    # Drop *_ctx columns if present
    drop_cols = [c for c in CTX_COLS if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # Build opponent map from schedule
    sched = pd.read_csv(SCHEDULE_FILE)
    req = {"home_team_id", "away_team_id"}
    missing = req - set(sched.columns)
    if missing:
        die(f"{SCHEDULE_FILE} missing required columns: {sorted(missing)}")

    # Coerce IDs
    for c in ["home_team_id", "away_team_id"]:
        sched[c] = pd.to_numeric(sched[c], errors="coerce").astype("Int64")

    home = sched[["home_team_id", "away_team_id"]].rename(
        columns={"home_team_id": "team_id", "away_team_id": "opponent_team_id"}
    )
    away = sched[["away_team_id", "home_team_id"]].rename(
        columns={"away_team_id": "team_id", "home_team_id": "opponent_team_id"}
    )
    long = pd.concat([home, away], ignore_index=True).dropna(subset=["team_id"])
    # Group to unique opponents per team_id
    grp = (
        long.groupby("team_id", dropna=True)["opponent_team_id"]
        .apply(lambda s: set(x for x in s.dropna().tolist()))
    )

    # Build mapping: only use if exactly one unique opponent; else NaN
    opp_map = {}
    for tid, opps in grp.items():
        if len(opps) == 1:
            opp_map[int(tid)] = int(next(iter(opps)))
        else:
            # ambiguous → leave unmapped
            continue

    # Coerce team_id in props and map opponent
    if "team_id" not in df.columns:
        die(f"{PROPS_FILE} missing required column: 'team_id'")
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")

    new_opp = df["team_id"].map(lambda x: opp_map.get(int(x)) if pd.notna(x) and int(x) in opp_map else pd.NA)
    df["opponent_team_id"] = pd.Series(new_opp, dtype="Int64")

    df.to_csv(PROPS_FILE, index=False)
    print(f"updated {PROPS_FILE}")

def clean_mega():
    if not MEGA_FILE.exists():
        die(f"Missing file: {MEGA_FILE}")
    if not LINEUPS_FILE.exists():
        die(f"Missing file: {LINEUPS_FILE}")

    df = pd.read_csv(MEGA_FILE)

    # role = "pitcher" for all
    df["role"] = "pitcher"

    # Map team_id from lineups by player_id, keep first occurrence
    lu = pd.read_csv(LINEUPS_FILE)
    if "player_id" not in lu.columns or "team_id" not in lu.columns:
        die(f"{LINEUPS_FILE} must contain 'player_id' and 'team_id'")

    map_df = (
        lu[["player_id", "team_id"]]
        .dropna(subset=["player_id", "team_id"])
        .drop_duplicates(subset=["player_id"], keep="first")
        .copy()
    )

    # Ensure numeric for mapping stability
    map_df["player_id"] = pd.to_numeric(map_df["player_id"], errors="coerce")
    map_df["team_id"]   = pd.to_numeric(map_df["team_id"], errors="coerce").astype("Int64")

    # Coerce target player_id to numeric for mapping
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")

    # Overwrite team_id where mapping available; keep existing otherwise
    new_team = df["player_id"].map(pd.Series(map_df["team_id"].values, index=map_df["player_id"]).to_dict())
    # new_team is Float/Int; cast to pandas NA-friendly Int64, then combine_first with existing coerced Int64
    new_team = pd.Series(new_team, index=df.index).astype("Int64")
    existing = pd.to_numeric(df.get("team_id"), errors="coerce").astype("Int64") if "team_id" in df.columns else pd.Series(pd.NA, index=df.index, dtype="Int64")
    df["team_id"] = new_team.combine_first(existing).astype("Int64")

    df.to_csv(MEGA_FILE, index=False)
    print(f"updated {MEGA_FILE}")

def main():
    clean_props()
    clean_mega()

if __name__ == "__main__":
    main()
