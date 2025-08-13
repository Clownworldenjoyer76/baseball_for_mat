# scripts/bet_prep_2.py
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from glob import glob

SCHED_IN   = Path("data/bets/mlb_sched.csv")
SOURCE_IN  = Path("data/_projections/batter_props_z_expanded.csv")  # will auto-pick newest * if this exact file not present
OUTPUT_OUT = Path("data/bets/prep/batter_props_bets.csv")

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df

def first_col(df: pd.DataFrame, *cands: str) -> str | None:
    """Return first matching column (case-insensitive), else None."""
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c in df.columns:
            return c
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def pick_source() -> Path:
    if SOURCE_IN.exists():
        return SOURCE_IN
    hits = sorted(glob("data/_projections/batter_props_z_expanded*.csv"))
    if not hits:
        raise SystemExit("❌ No batter_props_z_expanded*.csv found in data/_projections/")
    return Path(hits[-1])

def build_long_from_projections(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Build long 'prop/line/value' from whatever projection columns exist.
    Returns (long_df, used_sources_list).
    """
    # map source_column -> (prop, default_line)
    cand_map = {
        # hits
        "proj_hits": ("hits", 1.5),
        "total_hits_projection": ("hits", 1.5),

        # home runs
        "proj_hr": ("home_runs", 0.5),
        "avg_hr": ("home_runs", 0.5),

        # total bases
        "b_total_bases": ("total_bases", 1.5),
        "total_bases_projection": ("total_bases", 1.5),
    }

    present = [(c, *cand_map[c]) for c in cand_map if c in df.columns]
    used = [c for c, _, _ in present]
    if not present:
        # nothing to melt; return skeleton with empty prop/line/value
        out = df.copy()
        out["prop"] = ""
        out["line"] = np.nan
        out["value"] = np.nan
        return out, used

    id_cols = [c for c in ["player_id", "name", "team", "date", "game_id"] if c in df.columns]
    keep_cols = id_cols + used
    base = df[keep_cols].copy()

    long = base.melt(id_vars=id_cols, var_name="src_col", value_name="value")
    long = long.dropna(subset=["value"])
    long["value"] = pd.to_numeric(long["value"], errors="coerce")

    # attach (prop, default_line)
    map_df = pd.DataFrame(
        [{"src_col": c, "prop": prop, "default_line": dline} for c, prop, dline in present]
    )
    long = long.merge(map_df, on="src_col", how="left")
    long["line"] = long["default_line"]
    long["prop"] = long["prop"].fillna("").astype(str).str.strip()

    # tidy order
    preferred = ["player_id","name","team","date","game_id","prop","line","value","src_col"]
    ordered = [c for c in preferred if c in long.columns] + [c for c in long.columns if c not in preferred]
    return long[ordered], used

def main():
    # --- load
    if not SCHED_IN.exists():
        raise SystemExit(f"❌ Missing {SCHED_IN}")
    src_path = pick_source()

    mlb_sched_df = pd.read_csv(SCHED_IN)
    batter_df    = pd.read_csv(src_path)

    mlb_sched_df = norm_cols(mlb_sched_df)
    batter_df    = norm_cols(batter_df)

    created, kept, built, mapped = [], False, False, []

    # --- identifiers
    name_col = first_col(batter_df, "name", "player_name", "player") or "name"
    team_col = first_col(batter_df, "team", "team_name", "mlb_team", "team_code") or "team"
    pid_col  = first_col(batter_df, "player_id", "mlbam_id", "id") or "player_id"

    for col, label in [(name_col,"name"), (team_col,"team"), (pid_col,"player_id")]:
        if col not in batter_df.columns:
            batter_df[col] = ""
            created.append(label)

    batter_df = batter_df.rename(columns={name_col:"name", team_col:"team", pid_col:"player_id"})
    for c in ["name","team","player_id"]:
        batter_df[c] = batter_df[c].astype(str).str.strip()

    # --- schedule merge
    need = [c for c in ("home_team","away_team","date","game_id") if c not in mlb_sched_df.columns]
    if need:
        raise SystemExit(f"❌ mlb_sched.csv missing columns: {need}")

    away = mlb_sched_df.rename(columns={"away_team":"team"})[["team","date","game_id"]]
    home = mlb_sched_df.rename(columns={"home_team":"team"})[["team","date","game_id"]]
    sched = pd.concat([away, home], ignore_index=True)
    sched["team"] = sched["team"].astype(str).str.strip()

    batter_df = pd.merge(batter_df, sched[["team","date","game_id"]], on="team", how="left")

    # --- Case A: keep existing long format if present & populated
    has_prop = "prop" in batter_df.columns and batter_df["prop"].astype(str).str.strip().ne("").any()
    has_line = "line" in batter_df.columns and pd.to_numeric(batter_df["line"], errors="coerce").notna().any()
    has_val  = "value" in batter_df.columns and pd.to_numeric(batter_df["value"], errors="coerce").notna().any()

    if has_prop and has_line and has_val:
        kept = True
        batter_df["prop"]  = batter_df["prop"].astype(str).str.strip()
        batter_df["line"]  = pd.to_numeric(batter_df["line"], errors="coerce")
        batter_df["value"] = pd.to_numeric(batter_df["value"], errors="coerce")
        out = batter_df.copy()
    else:
        # --- Case B: build long from projections
        out, used = build_long_from_projections(batter_df)
        built = True
        if used:
            mapped = used

    # --- meta/defaults
    defaults = {
        "sport": "Baseball",
        "league": "MLB",
        "book": "",
        "timestamp": datetime.now().isoformat(),
        "result": "",
        "player": out["name"] if "name" in out.columns else "",
        "player_pos": "batter",
        "bet_type": "",
        "over_probability": np.nan,
        "prop_correct": "",
    }
    for col, val in defaults.items():
        if col not in out.columns:
            out[col] = val
            created.append(col)

    if "game_id" in out.columns:
        out["game_id"] = out["game_id"].astype(str)

    # --- tidy output order
    preferred = [
        "player_id","name","player","team","date","game_id",
        "prop","line","value","over_probability",
        "player_pos","bet_type","book","sport","league","timestamp","result",
    ]
    out = out[[c for c in preferred if c in out.columns] +
              [c for c in out.columns if c not in preferred]].copy()

    OUTPUT_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_OUT, index=False)

    # --- logs
    if kept:
        print("ℹ️ Kept existing prop/line/value from source (long format detected).")
    if built:
        print("ℹ️ Built prop/line/value from projections (wide → long).")
    if mapped:
        print("ℹ️ Projection columns used:", ", ".join(mapped))
    if created:
        print("ℹ️ Added defaults:", ", ".join(sorted(set(created))))
    print(f"✅ Source used: {src_path}")
    print(f"✅ Wrote: {OUTPUT_OUT} (rows={len(out)})")

if __name__ == "__main__":
    main()
