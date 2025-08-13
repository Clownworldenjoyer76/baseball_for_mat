# scripts/bet_prep_2.py
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

SCHED_IN   = Path("data/bets/mlb_sched.csv")
SOURCE_IN  = Path("data/_projections/batter_props_z_expanded.csv")
OUTPUT_OUT = Path("data/bets/prep/batter_props_bets.csv")

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df

def first_col(df: pd.DataFrame, candidates: list[str], default=None):
    # return the first matching column (case-insensitive), else default
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return default

def main():
    # --- load
    if not SCHED_IN.exists():
        raise SystemExit(f"❌ Missing {SCHED_IN}")
    if not SOURCE_IN.exists():
        raise SystemExit(f"❌ Missing {SOURCE_IN}")

    mlb_sched_df = pd.read_csv(SCHED_IN)
    batter_df    = pd.read_csv(SOURCE_IN)

    mlb_sched_df = norm_cols(mlb_sched_df)
    batter_df    = norm_cols(batter_df)

    created = []
    updated = []

    # --- map/ensure key identifiers
    name_col = first_col(batter_df, ["name", "player_name", "player"]) or "name"
    if name_col not in batter_df.columns:
        batter_df[name_col] = ""
        created.append("name")

    team_col = first_col(batter_df, ["team", "team_name", "mlb_team", "team_code"]) or "team"
    if team_col not in batter_df.columns:
        batter_df[team_col] = ""
        created.append("team")

    pid_col = first_col(batter_df, ["player_id", "mlbam_id", "id"]) or "player_id"
    if pid_col not in batter_df.columns:
        batter_df[pid_col] = ""
        created.append("player_id")

    # standardize to canonical column names we’ll write out
    batter_df = batter_df.rename(columns={
        name_col: "name",
        team_col: "team",
        pid_col:  "player_id",
    })

    # tidy types/whitespace
    for c in ["name", "team", "player_id"]:
        batter_df[c] = batter_df[c].astype(str).str.strip()

    # --- schedule merge to get date & game_id
    need = [c for c in ("home_team", "away_team", "date", "game_id") if c not in mlb_sched_df.columns]
    if need:
        raise SystemExit(f"❌ mlb_sched.csv missing columns: {need}")

    mlb_sched_away = mlb_sched_df.rename(columns={"away_team": "team"})[["team", "date", "game_id"]]
    mlb_sched_home = mlb_sched_df.rename(columns={"home_team": "team"})[["team", "date", "game_id"]]
    mlb_sched_merged = pd.concat([mlb_sched_away, mlb_sched_home], ignore_index=True)
    mlb_sched_merged["team"] = mlb_sched_merged["team"].astype(str).str.strip()

    batter_df = pd.merge(batter_df, mlb_sched_merged[["team", "date", "game_id"]],
                         on="team", how="left")

    # --- required columns for downstream script
    # PROP: keep existing if present; else backfill from prop_type; else create empty
    if "prop" not in batter_df.columns:
        legacy_prop_type = first_col(batter_df, ["prop_type"])
        if legacy_prop_type:
            batter_df["prop"] = batter_df[legacy_prop_type].astype(str).str.strip()
            created.append("prop")
        else:
            batter_df["prop"] = ""
            created.append("prop")
    else:
        # if 'prop' exists but blank/NA while 'prop_type' exists, backfill only where blank
        legacy_prop_type = first_col(batter_df, ["prop_type"])
        if legacy_prop_type:
            mask_blank = batter_df["prop"].isna() | (batter_df["prop"].astype(str).str.strip() == "")
            if mask_blank.any():
                batter_df.loc[mask_blank, "prop"] = (
                    batter_df.loc[mask_blank, legacy_prop_type].astype(str).str.strip()
                )
                updated.append("prop(backfilled_from_prop_type)")
    batter_df["prop"] = batter_df["prop"].fillna("").astype(str).str.strip()

    # LINE: keep existing numeric 'line' if present; otherwise backfill from prop_line; else NaN
    src_prop_line = first_col(batter_df, ["prop_line"])
    if "line" not in batter_df.columns:
        if src_prop_line:
            batter_df["line"] = pd.to_numeric(batter_df[src_prop_line], errors="coerce")
            created.append("line")
        else:
            batter_df["line"] = np.nan
            created.append("line")
    else:
        # coerce and backfill blanks from prop_line if available
        batter_df["line"] = pd.to_numeric(batter_df["line"], errors="coerce")
        if src_prop_line:
            src_num = pd.to_numeric(batter_df[src_prop_line], errors="coerce")
            mask_na = batter_df["line"].isna()
            if mask_na.any():
                batter_df.loc[mask_na, "line"] = src_num[mask_na]
                if src_num[mask_na].notna().any():
                    updated.append("line(backfilled_from_prop_line)")

    # VALUE: keep existing numeric 'value' if present; otherwise create NaN
    if "value" not in batter_df.columns:
        batter_df["value"] = np.nan
        created.append("value")
    batter_df["value"] = pd.to_numeric(batter_df["value"], errors="coerce")

    # optional / convenience fields your site uses
    defaults = {
        "sport": "Baseball",
        "league": "MLB",
        "book": "",
        "timestamp": datetime.now().isoformat(),
        "result": "",
        "player": batter_df["name"],  # convenience alias
        "player_pos": "batter",
        "bet_type": "",
        "over_probability": np.nan,
        "prop_correct": "",
    }
    for col, val in defaults.items():
        if col not in batter_df.columns:
            batter_df[col] = val
            created.append(col)

    # game_id as string for stability in CSV
    if "game_id" in batter_df.columns:
        batter_df["game_id"] = batter_df["game_id"].astype(str)

    # --- order columns (nice-to-have)
    preferred = [
        "player_id", "name", "player", "team",
        "date", "game_id",
        "prop", "line", "value",
        "over_probability",
        "player_pos", "bet_type", "book",
        "sport", "league", "timestamp", "result",
    ]
    ordered = [c for c in preferred if c in batter_df.columns]
    rest = [c for c in batter_df.columns if c not in ordered]
    out = batter_df[ordered + rest].copy()

    OUTPUT_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_OUT, index=False)

    if created:
        print("ℹ️ Created columns:", ", ".join(sorted(set(created))))
    if updated:
        print("ℹ️ Backfilled columns:", ", ".join(sorted(set(updated))))
    print(f"✅ Wrote: {OUTPUT_OUT} (rows={len(out)})")

if __name__ == "__main__":
    main()
