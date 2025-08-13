# scripts/bet_prep_2.py
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

SCHED_IN   = Path("data/bets/mlb_sched.csv")
SOURCE_IN  = Path("data/_projections/batter_props_z_expanded.csv")  # if you sometimes write " 2.csv", see the auto-pick block below
OUTPUT_OUT = Path("data/bets/prep/batter_props_bets.csv")

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df

def first_col(df: pd.DataFrame, *cands: str) -> str | None:
    """Return first matching column name (case-insensitive)."""
    lower_map = {c.lower(): c for c in df.columns}
    for c in cands:
        if c in df.columns:
            return c
        lc = c.lower()
        if lc in lower_map:
            return lower_map[lc]
    return None

def choose_source_path() -> Path:
    # Prefer canonical; if missing, pick the newest batter_props_z_expanded*.csv
    if SOURCE_IN.exists():
        return SOURCE_IN
    from glob import glob
    hits = sorted(glob("data/_projections/batter_props_z_expanded*.csv"))
    if not hits:
        raise SystemExit("❌ No batter_props_z_expanded*.csv found in data/_projections/")
    return Path(hits[-1])

def main():
    # --- load
    if not SCHED_IN.exists():
        raise SystemExit(f"❌ Missing {SCHED_IN}")
    src_path = choose_source_path()

    mlb_sched_df = pd.read_csv(SCHED_IN)
    batter_df    = pd.read_csv(src_path)

    mlb_sched_df = norm_cols(mlb_sched_df)
    batter_df    = norm_cols(batter_df)

    created, updated, mapped = [], [], []

    # --- map/ensure identifiers
    name_col = first_col(batter_df, "name", "player_name", "player") or "name"
    if name_col not in batter_df.columns:
        batter_df[name_col] = ""
        created.append("name")

    team_col = first_col(batter_df, "team", "team_name", "mlb_team", "team_code") or "team"
    if team_col not in batter_df.columns:
        batter_df[team_col] = ""
        created.append("team")

    pid_col  = first_col(batter_df, "player_id", "mlbam_id", "id") or "player_id"
    if pid_col not in batter_df.columns:
        batter_df[pid_col] = ""
        created.append("player_id")

    batter_df = batter_df.rename(columns={name_col: "name", team_col: "team", pid_col: "player_id"})
    for c in ["name", "team", "player_id"]:
        batter_df[c] = batter_df[c].astype(str).str.strip()

    # --- schedule merge
    need = [c for c in ("home_team", "away_team", "date", "game_id") if c not in mlb_sched_df.columns]
    if need:
        raise SystemExit(f"❌ mlb_sched.csv missing columns: {need}")

    away = mlb_sched_df.rename(columns={"away_team": "team"})[["team", "date", "game_id"]]
    home = mlb_sched_df.rename(columns={"home_team": "team"})[["team", "date", "game_id"]]
    sched = pd.concat([away, home], ignore_index=True)
    sched["team"] = sched["team"].astype(str).str.strip()

    batter_df = pd.merge(batter_df, sched[["team", "date", "game_id"]], on="team", how="left")

    # --- prop mapping (stick with 'prop')
    if "prop" in batter_df.columns:
        # keep as-is but trim
        batter_df["prop"] = batter_df["prop"].astype(str).str.strip()
    else:
        src_prop = first_col(batter_df, "prop_type", "propname", "market", "market_name")
        if src_prop:
            batter_df["prop"] = batter_df[src_prop].astype(str).str.strip()
            mapped.append(f"prop<-{src_prop}")
        else:
            batter_df["prop"] = ""
            created.append("prop")

    # --- line mapping (numeric)
    if "line" in batter_df.columns:
        batter_df["line"] = pd.to_numeric(batter_df["line"], errors="coerce")
    else:
        src_line = first_col(batter_df, "prop_line", "line_value", "line_odds", "line")
        if src_line:
            batter_df["line"] = pd.to_numeric(batter_df[src_line], errors="coerce")
            mapped.append(f"line<-{src_line}")
        else:
            batter_df["line"] = np.nan
            created.append("line")

    # --- value mapping (numeric projection/expectation)
    # Try the most common names in your pipeline
    value_srcs = [
        "value", "projection", "expected", "proj", "mean",
        "total_bases_projection", "total_hits_projection", "avg_hr",
    ]
    if "value" in batter_df.columns:
        batter_df["value"] = pd.to_numeric(batter_df["value"], errors="coerce")
    else:
        found_val = None
        for cand in value_srcs:
            src_val = first_col(batter_df, cand)
            if src_val is not None:
                batter_df["value"] = pd.to_numeric(batter_df[src_val], errors="coerce")
                mapped.append(f"value<-{src_val}")
                found_val = src_val
                break
        if found_val is None:
            batter_df["value"] = np.nan
            created.append("value")

    # convenience / site fields
    defaults = {
        "sport": "Baseball",
        "league": "MLB",
        "book": "",
        "timestamp": datetime.now().isoformat(),
        "result": "",
        "player": batter_df["name"],
        "player_pos": "batter",
        "bet_type": "",
        "over_probability": np.nan,
        "prop_correct": "",
    }
    for col, val in defaults.items():
        if col not in batter_df.columns:
            batter_df[col] = val
            created.append(col)

    if "game_id" in batter_df.columns:
        batter_df["game_id"] = batter_df["game_id"].astype(str)

    # column order
    preferred = [
        "player_id","name","player","team","date","game_id",
        "prop","line","value","over_probability",
        "player_pos","bet_type","book","sport","league","timestamp","result",
    ]
    out = batter_df[[c for c in preferred if c in batter_df.columns] +
                    [c for c in batter_df.columns if c not in preferred]].copy()

    OUTPUT_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_OUT, index=False)

    if created:
        print("ℹ️ Created columns:", ", ".join(sorted(set(created))))
    if mapped:
        print("ℹ️ Mapped columns:", ", ".join(mapped))
    print(f"✅ Source used: {src_path}")
    print(f"✅ Wrote: {OUTPUT_OUT} (rows={len(out)})")

if __name__ == "__main__":
    main()
