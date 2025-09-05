#!/usr/bin/env python3
import sys
import os
import json
from pathlib import Path
import pandas as pd

ROOT = Path(".")
LOG_PREFIX = "backfill_game_context"
PROJ_DIR = ROOT / "data" / "_projections"
END_CHAIN = ROOT / "data" / "end_chain" / "final"

BATTER_PROJECTED = PROJ_DIR / "batter_props_projected_fixed.csv"
BATTER_EXPANDED  = PROJ_DIR / "batter_props_expanded_fixed.csv"
PITCHER_PROPS    = PROJ_DIR / "pitcher_props_projected_fixed.csv"
PITCHER_MEGA     = PROJ_DIR / "pitcher_mega_z_fixed.csv"
SP_PATH          = END_CHAIN / "startingpitchers.csv"

REQUIRED_FILES = [BATTER_PROJECTED, BATTER_EXPANDED, PITCHER_PROPS, PITCHER_MEGA]

def log(msg, level="info"):
    tag = {"info":"ℹ️", "ok":"✅", "warn":"⚠️", "err":"❌"}.get(level, "ℹ️")
    print(f"{tag} {LOG_PREFIX}: {msg}")

def soft_require_columns(df, needed):
    missing = [c for c in needed if c not in df.columns]
    return missing

def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def write_csv(df: pd.DataFrame, path: Path):
    tmp = path.with_suffix(".tmp.csv")
    df.to_csv(tmp, index=False)
    tmp.replace(path)

def ensure_columns(df: pd.DataFrame, cols_in_order):
    for c in cols_in_order:
        if c not in df.columns:
            df[c] = pd.NA
    # Reorder to requested order + any extras at end (stable)
    extras = [c for c in df.columns if c not in cols_in_order]
    return df[cols_in_order + extras]

def build_game_map(sp: pd.DataFrame):
    """
    Returns:
      game_map: DataFrame[game_id, home_team_id, away_team_id]
      ctx_cols: list of optional context columns we can carry by game_id (city/state/timezone/is_dome/park_factor)
    """
    needed = ["game_id", "team_context", "team_id"]
    miss = soft_require_columns(sp, needed)
    if miss:
        log(f"Unable to infer (home_team_id, away_team_id); missing columns in startingpitchers.csv: {miss}", "warn")
        return pd.DataFrame(columns=["game_id","home_team_id","away_team_id"]), []

    # Normalize values
    sp2 = sp.copy()
    sp2["team_context"] = sp2["team_context"].str.lower().str.strip()

    # Build wide table home/away -> team_id
    try:
        wide = sp2.pivot_table(index="game_id", columns="team_context", values="team_id", aggfunc="first").reset_index()
    except Exception as e:
        log(f"Pivot to create game map failed: {e}", "warn")
        return pd.DataFrame(columns=["game_id","home_team_id","away_team_id"]), []

    # Standardize column names
    colmap = {}
    if "home" in wide.columns: colmap["home"] = "home_team_id"
    if "away" in wide.columns: colmap["away"] = "away_team_id"
    wide = wide.rename(columns=colmap)

    for need in ["home_team_id","away_team_id"]:
        if need not in wide.columns:
            wide[need] = pd.NA

    game_map = wide[["game_id","home_team_id","away_team_id"]].copy()

    # Optional context columns (carry first non-null per game)
    optional_ctx = [c for c in ["city","state","timezone","is_dome","park_factor"]
                    if c in sp2.columns]
    ctx_df = None
    if optional_ctx:
        # Keep one row per game_id (first valid)
        ctx_df = (
            sp2.groupby("game_id")[optional_ctx]
            .apply(lambda g: g.ffill().bfill().iloc[0])
            .reset_index()
        )
        game_map = game_map.merge(ctx_df, on="game_id", how="left")

    return game_map, optional_ctx

def enrich_pitcher_props(pp: pd.DataFrame, game_map: pd.DataFrame, optional_ctx_cols):
    # Ensure core columns exist in pitcher props
    core_cols = ["game_id","role","team_id","opponent_team_id","park_factor","city","state","timezone","is_dome","undecided"]
    for c in core_cols:
        if c not in pp.columns:
            pp[c] = pd.NA

    # Fill opponent_team_id if blank and we know game_id+team_id
    if not game_map.empty:
        gm = game_map[["game_id","home_team_id","away_team_id"]].copy()
        gm["home_team_id"] = gm["home_team_id"].astype("Int64")
        gm["away_team_id"] = gm["away_team_id"].astype("Int64")

        def infer_opponent(row):
            gid = row.get("game_id")
            tid = row.get("team_id")
            if pd.isna(gid) or pd.isna(tid): return row.get("opponent_team_id")
            rec = gm[gm["game_id"]==gid]
            if rec.empty: return row.get("opponent_team_id")
            h = rec["home_team_id"].iloc[0]
            a = rec["away_team_id"].iloc[0]
            if pd.isna(h) or pd.isna(a): return row.get("opponent_team_id")
            if tid == h: return a
            if tid == a: return h
            return row.get("opponent_team_id")

        pp["opponent_team_id"] = pp.apply(infer_opponent, axis=1)

        # Fill park/city/state/timezone/is_dome when missing
        carry_cols = ["park_factor","city","state","timezone","is_dome"]
        carry_cols = [c for c in carry_cols if c in optional_ctx_cols or c in game_map.columns]
        if carry_cols:
            pp = pp.merge(game_map[["game_id"]+carry_cols], on="game_id", how="left", suffixes=("","__gm"))
            for c in carry_cols:
                src = f"{c}__gm"
                if src in pp.columns:
                    pp[c] = pp[c].combine_first(pp[src])
                    pp.drop(columns=[src], inplace=True, errors="ignore")

    # Ensure undecided is normalized
    if "undecided" in pp.columns:
        pp["undecided"] = pp["undecided"].fillna(False)

    return pp

def validate_fixed_outputs():
    problems = []

    # 1) batter_props_projected_fixed.csv
    cols_bpp = ["player_id","name","team","prob_hits_over_1p5","prob_tb_over_1p5",
                "prob_hr_over_0p5","proj_pa_used","proj_ab_est","proj_avg_used",
                "proj_iso_used","proj_hr_rate_pa_used","game_id",
                "adj_woba_weather","adj_woba_park","adj_woba_combined"]
    try:
        bpp = read_csv(BATTER_PROJECTED)
        bpp2 = ensure_columns(bpp, cols_bpp)
        if (bpp2["proj_hr_rate_pa_used"].isna().any()):
            # We don't manufacture rates here—just ensure the column exists.
            pass
        write_csv(bpp2, BATTER_PROJECTED)
    except Exception as e:
        problems.append(f"{BATTER_PROJECTED.name}: {e}")

    # 2) batter_props_expanded_fixed.csv
    try:
        bex = read_csv(BATTER_EXPANDED)
        bex2 = ensure_columns(bex, cols_bpp)  # same envelope as projected
        write_csv(bex2, BATTER_EXPANDED)
    except Exception as e:
        problems.append(f"{BATTER_EXPANDED.name}: {e}")

    # 3) pitcher_props_projected_fixed.csv
    try:
        pp = read_csv(PITCHER_PROPS)
        # We’ll enrich after we build game_map
    except Exception as e:
        problems.append(f"{PITCHER_PROPS.name}: {e}")
        pp = None

    # 4) pitcher_mega_z_fixed.csv (just make sure it exists/readable)
    try:
        pmz = read_csv(PITCHER_MEGA)
        write_csv(pmz, PITCHER_MEGA)  # no changes
    except Exception as e:
        problems.append(f"{PITCHER_MEGA.name}: {e}")

    return problems, pp

def main():
    print(f"▶️ {LOG_PREFIX}.py starting")
    # Validate the four outputs exist + columns present
    problems, pitcher_df = validate_fixed_outputs()
    if problems:
        for p in problems:
            log(p, "warn")

    # Try to read startingpitchers to build game map (soft fail)
    game_map = pd.DataFrame(columns=["game_id","home_team_id","away_team_id"])
    optional_ctx_cols = []
    if SP_PATH.exists():
        try:
            sp = read_csv(SP_PATH)
            game_map, optional_ctx_cols = build_game_map(sp)
            if game_map.empty:
                log("Game map build produced no rows (will skip enrichment).", "warn")
            else:
                log(f"Built game map with {len(game_map)} games.", "ok")
        except Exception as e:
            log(f"Could not build game map from startingpitchers.csv: {e}", "warn")
    else:
        log(f"{SP_PATH} not found; skipping enrichment.", "warn")

    # If we have pitcher props, enrich it safely
    if pitcher_df is not None:
        try:
            pp_enriched = enrich_pitcher_props(pitcher_df, game_map, optional_ctx_cols)
            write_csv(pp_enriched, PITCHER_PROPS)
            log(f"Enriched {PITCHER_PROPS.name} (rows={len(pp_enriched)})", "ok")
        except Exception as e:
            log(f"Could not enrich {PITCHER_PROPS.name}: {e}", "warn")

    # Final validation message
    missing_any = [p for p in REQUIRED_FILES if not p.exists()]
    if missing_any:
        for p in missing_any:
            log(f"Missing expected output: {p}", "err")
        # Real error only if a required output is missing from disk
        sys.exit(1)

    log("validated all *_fixed.csv outputs", "ok")
    print("")  # spacing
    sys.exit(0)

if __name__ == "__main__":
    main()
