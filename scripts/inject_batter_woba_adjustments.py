#!/usr/bin/env python3
# scripts/inject_batter_woba_adjustments.py
#
# Purpose:
#   Update the following two files IN-PLACE by injecting the columns:
#     - adj_woba_weather
#     - adj_woba_park
#     - adj_woba_combined
#
# Targets:
#   - data/_projections/batter_props_expanded_final.csv
#   - data/_projections/batter_props_projected_final.csv
#
# Source (match on player_id):
#   - data/adjusted/batters_away_weather_park.csv
#   - data/adjusted/batters_home_weather_park.csv
#
# Rules:
#   - No assumptions or fabricated values.
#   - If both away and home have values for a player_id and they differ, mark as NA and count a conflict.
#   - If only one source has values, use that one.
#   - If neither has values, leave as NA.
#   - Overwrite existing adj_woba_* columns in the targets if they exist.

from pathlib import Path
import sys
import pandas as pd

# ===== Paths =====
TARGETS = [
    Path("data/_projections/batter_props_expanded_final.csv"),
    Path("data/_projections/batter_props_projected_final.csv"),
]
AWAY_SRC = Path("data/adjusted/batters_away_weather_park.csv")
HOME_SRC = Path("data/adjusted/batters_home_weather_park.csv")

REQ_TARGET_KEY = "player_id"
REQ_SRC_KEY = "player_id"
REQ_COLS = ["adj_woba_weather", "adj_woba_park", "adj_woba_combined"]

def die(msg: str) -> None:
    sys.stderr.write(f"ERROR: {msg}\n")
    sys.exit(1)

def load_source(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"Missing source file: {path}")
    df = pd.read_csv(path)
    if REQ_SRC_KEY not in df.columns:
        die(f"{path} missing required column: '{REQ_SRC_KEY}'")
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        die(f"{path} missing required columns: {missing}")
    # Keep only needed columns
    return df[[REQ_SRC_KEY] + REQ_COLS].copy()

def resolve_from_sources(away: pd.DataFrame, home: pd.DataFrame) -> pd.DataFrame:
    # Suffix to distinguish columns after merge
    away_sfx = "_away"
    home_sfx = "_home"

    # Merge away & home on player_id (outer to capture any)
    merged = away.merge(home, on=REQ_SRC_KEY, how="outer", suffixes=(away_sfx, home_sfx))

    # For each target column, choose value per rules
    out = merged[[REQ_SRC_KEY]].copy()
    conflicts = {c: 0 for c in REQ_COLS}
    only_away = {c: 0 for c in REQ_COLS}
    only_home = {c: 0 for c in REQ_COLS}
    all_na = {c: 0 for c in REQ_COLS}

    for col in REQ_COLS:
        ca = f"{col}{away_sfx}"
        ch = f"{col}{home_sfx}"

        if ca not in merged.columns or ch not in merged.columns:
            die("Internal merge error: expected suffixed columns missing.")

        a = merged[ca]
        h = merged[ch]

        # Both NA → NA
        both_na_mask = a.isna() & h.isna()
        # Only away present
        only_a_mask = a.notna() & h.isna()
        # Only home present
        only_h_mask = a.isna() & h.notna()
        # Both present and EXACTLY equal
        both_eq_mask = a.notna() & h.notna() & (a == h)
        # Both present and different → conflict → NA
        conflict_mask = a.notna() & h.notna() & (a != h)

        # Build resolved column
        resolved = pd.Series(pd.NA, index=merged.index, dtype="Float64")
        resolved.loc[only_a_mask] = a.loc[only_a_mask].astype("Float64")
        resolved.loc[only_h_mask] = h.loc[only_h_mask].astype("Float64")
        resolved.loc[both_eq_mask] = a.loc[both_eq_mask].astype("Float64")
        # conflicts remain NA per rules

        # Stats
        conflicts[col] = int(conflict_mask.sum())
        only_away[col] = int(only_a_mask.sum())
        only_home[col] = int(only_h_mask.sum())
        all_na[col] = int(both_na_mask.sum())

        out[col] = resolved

    # Reporting summary to stdout
    total_ids = len(out)
    print("=== inject_batter_woba_adjustments: resolution summary (by column) ===")
    print(f"player_ids considered: {total_ids}")
    for col in REQ_COLS:
        print(
            f"{col}: only_away={only_away[col]}, only_home={only_home[col]}, "
            f"both_equal={int((~out[col].isna()).sum() - only_away[col] - only_home[col])}, "
            f"conflicts={conflicts[col]}, all_na={all_na[col]}"
        )
    return out

def update_target(target_path: Path, resolved: pd.DataFrame) -> None:
    if not target_path.exists():
        die(f"Missing target file: {target_path}")
    df = pd.read_csv(target_path)

    if REQ_TARGET_KEY not in df.columns:
        die(f"{target_path} missing required column: '{REQ_TARGET_KEY}'")

    # Left-join resolved adj_woba_* onto target
    # Ensure player_id dtypes are comparable
    # Force numeric where possible; leave non-numeric as-is (but merge keys must align)
    try:
        df_pid = pd.to_numeric(df[REQ_TARGET_KEY], errors="raise")
        res_pid = pd.to_numeric(resolved[REQ_TARGET_KEY], errors="raise")
        df_key = df_pid
        res_key = res_pid
    except Exception:
        # Fallback to string match if numeric coercion fails for either
        df_key = df[REQ_TARGET_KEY].astype(str)
        res_key = resolved[REQ_TARGET_KEY].astype(str)

    df = df.copy()
    df["_merge_key"] = df_key
    tmp = resolved.copy()
    tmp["_merge_key"] = res_key

    df_merged = df.merge(
        tmp.drop(columns=[REQ_TARGET_KEY]),
        on="_merge_key",
        how="left",
        suffixes=("", "_resolved"),
        validate="m:1"
    )

    # Overwrite or create target columns strictly from resolved values
    for col in REQ_COLS:
        col_res = f"{col}"
        if col_res not in df_merged.columns:
            # create if not present
            df_merged[col_res] = pd.Series(pd.NA, index=df_merged.index, dtype="Float64")
        # values come from the columns brought by merge (same name)
        # After merge, columns have same base name; ensure we pick the right one:
        # If there is a duplicate due to existing column, the left side kept original.
        # So pull from the right via suffix "_resolved" if present, else from base.
        cand_right = f"{col}_resolved"
        if cand_right in df_merged.columns:
            df_merged[col_res] = df_merged[cand_right].astype("Float64")
            df_merged.drop(columns=[cand_right], inplace=True)

    # Cleanup helper key
    df_merged.drop(columns=["_merge_key"], inplace=True)

    # Write back
    df_merged.to_csv(target_path, index=False)
    print(f"✅ Updated {target_path} with adj_woba_* (rows={len(df_merged)})")

def main() -> None:
    away = load_source(AWAY_SRC)
    home = load_source(HOME_SRC)
    resolved = resolve_from_sources(away, home)

    for tgt in TARGETS:
        update_target(tgt, resolved)

if __name__ == "__main__":
    main()
