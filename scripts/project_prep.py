#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def project_prep():
    ROOT = Path(".")
    DATA = ROOT / "data"

    # Input files
    schedule_file = DATA / "cleaned" / "mlb_sched.csv"
    pitchers_file = DATA / "cleaned" / "pitchers.csv"
    stadium_file = DATA / "stadium_master.csv"

    # Load
    sched = pd.read_csv(schedule_file, dtype=str)
    pitchers = pd.read_csv(pitchers_file, dtype=str)
    stadiums = pd.read_csv(stadium_file, dtype=str)

    # Merge schedule with pitchers
    merged = sched.merge(
        pitchers,
        left_on=["home_team_id", "away_team_id", "game_id"],
        right_on=["home_team_id", "away_team_id", "game_id"],
        how="left",
    )

    # Stadium join
    venue_cols = ["team_id", "team_name", "venue", "city", "state",
                  "timezone", "is_dome", "latitude", "longitude", "home_team_stadium"]
    stadium_sub = stadiums[venue_cols].drop_duplicates("team_id")
    merged = merged.merge(stadium_sub, left_on="home_team_id", right_on="team_id", how="left")

    # Outputs
    out1 = DATA / "end_chain" / "final" / "startingpitchers.csv"
    out2 = DATA / "raw" / "startingpitchers_with_opp_context.csv"
    out1.parent.mkdir(parents=True, exist_ok=True)
    out2.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out1, index=False)
    merged.to_csv(out2, index=False)

    # === NEW: long/normalized file for pitcher pipeline ===
    sp = pd.read_csv(out1, dtype=str)
    ctx = pd.read_csv(out2, dtype=str)

    common_cols = [
        "game_id", "game_time", "park_factor",
        "home_team_id", "away_team_id", "home_team", "away_team",
        "team_name", "venue", "city", "state", "timezone", "is_dome",
        "latitude", "longitude", "home_team_stadium"
    ]

    home_cols = {
        "player_id_home": "player_id",
        "pitcher_home_id": "player_id_fallback",
        "home_team_id": "team_id",
        "away_team_id": "opponent_team_id",
    }
    away_cols = {
        "player_id_away": "player_id",
        "pitcher_away_id": "player_id_fallback",
        "away_team_id": "team_id",
        "home_team_id": "opponent_team_id",
    }

    def build_side(df, side_cols, is_home_flag):
        side = df.rename(columns=side_cols).copy()
        side["is_home"] = "1" if is_home_flag else "0"
        side["player_id"] = side["player_id"].fillna("")
        if "player_id_fallback" in side.columns:
            side.loc[side["player_id"] == "", "player_id"] = side["player_id_fallback"]
            side = side.drop(columns=["player_id_fallback"])
        return side

    home_long = build_side(ctx, home_cols, True)
    away_long = build_side(ctx, away_cols, False)

    keep_cols = ["player_id", "team_id", "opponent_team_id", "game_id", "is_home"]
    keep_cols += [c for c in common_cols if c in home_long.columns]
    starters_long = pd.concat([home_long, away_long], ignore_index=True)[keep_cols].drop_duplicates()
    starters_long = starters_long.astype(str)
    starters_long = starters_long[starters_long["player_id"].str.len() > 0]

    out_long = DATA / "raw" / "startingpitchers_for_pitcher_pipeline.csv"
    out_long.parent.mkdir(parents=True, exist_ok=True)
    starters_long.to_csv(out_long, index=False)

    print(f"project_prep: wrote {out1} and {out2} (rows={len(merged)})")
    print(f"project_prep: wrote {out_long} (rows={len(starters_long)})")

def main():
    project_prep()

if __name__ == "__main__":
    main()
