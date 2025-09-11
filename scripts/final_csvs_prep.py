#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd

# ----- Config (repo-relative paths) -----
BPP_PATH = Path("data/_projections/batter_props_projected_final.csv")
BPX_PATH = Path("data/_projections/batter_props_expanded_final.csv")
PPP_PATH = Path("data/_projections/pitcher_props_projected_final.csv")
BATTERS_TODAY_PATH = Path("data/cleaned/batters_today.csv")
MLB_SCHED_PATH = Path("data/bets/mlb_sched.csv")

# ----- IO helpers -----
def read_csv_safe(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        print(f"[MISS] {path}")
        return None
    try:
        df = pd.read_csv(path)
        print(f"[LOAD] {path} rows={len(df)}")
        return df
    except Exception as e:
        print(f"[ERR ] reading {path}: {e!r}")
        return None

def write_csv_safe(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[SAVE] {path} rows={len(df)}")

# ----- Column utilities -----
def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            print(f"[ADD ] Creating empty column '{c}'")
            df[c] = None
        else:
            print(f"[KEEP] Column '{c}' exists")
    return df

def to_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df

def drop_if_exists(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    existing = [c for c in cols if c in df.columns]
    if existing:
        df = df.drop(columns=existing)
        print(f"[CLEAN] Dropped columns: {existing}")
    return df

# ----- Injection helper -----
def inject_column_by_key(
    target_df: pd.DataFrame,
    source_df: pd.DataFrame,
    on_col: str,
    inject_col: str,
) -> pd.DataFrame:
    """
    Left-merge source_df[[on_col, inject_col]] into target_df on on_col.
    Aligns dtypes to string for join stability. Resolves *_x/_y into a single inject_col.
    """
    if on_col not in target_df.columns:
        raise KeyError(f"target missing join key: {on_col}")
    if on_col not in source_df.columns or inject_col not in source_df.columns:
        raise KeyError(f"source missing required columns: {on_col}, {inject_col}")

    # Align dtypes
    target_df = to_str(target_df, [on_col])
    source_df = to_str(source_df, [on_col, inject_col])

    print(f"[JOIN] Inject '{inject_col}' into target via '{on_col}'")
    merged = pd.merge(
        target_df,
        source_df[[on_col, inject_col]].drop_duplicates(),
        on=on_col,
        how="left",
        suffixes=("", "_src"),
    )

    # If inject_col already existed in target, prefer the source column
    if f"{inject_col}_src" in merged.columns:
        merged[inject_col] = merged[f"{inject_col}_src"]
        merged = merged.drop(columns=[f"{inject_col}_src"])
    return merged

# ----- Main workflow -----
def main():
    print("--- Starting final_csvs_prep.py ---")

    # Load primary targets
    bpp = read_csv_safe(BPP_PATH)  # projected batters (needs team_id, game_id)
    bpx = read_csv_safe(BPX_PATH)  # expanded batters (needs team_id, game_id for later script)
    ppp = read_csv_safe(PPP_PATH)  # projected pitchers (has team_id/opponent_team_id; may have messy dup cols)

    # Ensure minimal columns exist to avoid KeyErrors later
    if bpp is not None:
        bpp = ensure_columns(bpp, ["team_id", "game_id"])
        write_csv_safe(bpp, BPP_PATH)
    if bpx is not None:
        bpx = ensure_columns(bpx, ["team_id", "game_id"])  # FIX: add team_id here
        write_csv_safe(bpx, BPX_PATH)
    if ppp is not None:
        ppp = ensure_columns(ppp, ["game_id"])  # team_id already present in most cases
        write_csv_safe(ppp, PPP_PATH)

    # 1) Inject team_id into BOTH batter files using batters_today on player_id
    btoday = read_csv_safe(BATTERS_TODAY_PATH)
    if btoday is not None:
        # minimal schema check
        need = {"player_id", "team_id"}
        if need.issubset(btoday.columns):
            if bpp is not None:
                bpp = read_csv_safe(BPP_PATH)  # refresh after last write
                bpp = inject_column_by_key(bpp, btoday, on_col="player_id", inject_col="team_id")
                write_csv_safe(bpp, BPP_PATH)
            if bpx is not None:
                bpx = read_csv_safe(BPX_PATH)
                bpx = inject_column_by_key(bpx, btoday, on_col="player_id", inject_col="team_id")
                write_csv_safe(bpx, BPX_PATH)
        else:
            print(f"[WARN] {BATTERS_TODAY_PATH} missing columns {sorted(need)}. Skipping team_id injection for batters.")
    else:
        print(f"[WARN] Missing {BATTERS_TODAY_PATH}. Skipping team_id injection for batters.")

    # 2) Build simplified schedule (game_id, team_id) from mlb_sched
    sched = read_csv_safe(MLB_SCHED_PATH)
    simplified = None
    if sched is not None:
        if {"game_id", "home_team_id", "away_team_id"}.issubset(sched.columns):
            home = sched[["game_id", "home_team_id"]].rename(columns={"home_team_id": "team_id"})
            away = sched[["game_id", "away_team_id"]].rename(columns={"away_team_id": "team_id"})
            simplified = pd.concat([home, away], ignore_index=True).drop_duplicates().reset_index(drop=True)
            simplified = to_str(simplified, ["game_id", "team_id"])
            print(f"[MAKE] simplified schedule rows={len(simplified)}")
        else:
            print(f"[WARN] {MLB_SCHED_PATH} missing required columns for schedule join. Skipping game_id injection.")
    else:
        print(f"[WARN] Missing {MLB_SCHED_PATH}. Skipping game_id injection.")

    # 3) Inject game_id into BOTH batter files and pitchers file using (team_id -> game_id)
    if simplified is not None:
        if bpp is not None:
            bpp = read_csv_safe(BPP_PATH)
            # Ensure team_id string for join
            bpp = to_str(bpp, ["team_id"])
            bpp = inject_column_by_key(bpp, simplified, on_col="team_id", inject_col="game_id")
            write_csv_safe(bpp, BPP_PATH)

        if bpx is not None:
            bpx = read_csv_safe(BPX_PATH)
            bpx = to_str(bpx, ["team_id"])
            bpx = inject_column_by_key(bpx, simplified, on_col="team_id", inject_col="game_id")
            write_csv_safe(bpx, BPX_PATH)

        if ppp is not None:
            ppp = read_csv_safe(PPP_PATH)
            ppp = to_str(ppp, ["team_id"])
            ppp = inject_column_by_key(ppp, simplified, on_col="team_id", inject_col="game_id")
            # Optional cleanup: drop known duplicate artifact columns if present
            ppp = drop_if_exists(ppp, [
                "team_id_x", "team_id_y", "team_id_sp",
                "game_id_sp", "game_id_sp.1"
            ])
            write_csv_safe(ppp, PPP_PATH)

    print("--- Script finished. ---")

if __name__ == "__main__":
    main()
