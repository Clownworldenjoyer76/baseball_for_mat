#!/usr/bin/env python3
# scripts/normalize_batters_for_builder.py
# Purpose: Ensure data/tagged/batters_normalized.csv has the strict columns required by
# build_expanded_batter_props.py (Mode B). Augments IN PLACE, with wide alias support
# and deterministic fallbacks. Fails fast with actionable errors.

import sys
from pathlib import Path
import pandas as pd
import numpy as np

SRC = Path("data/tagged/batters_normalized.csv")

# Columns the builder requires (exact names)
REQ = [
    "player_id","name","team",
    "pa","g","ab","slg",
    "season_hits","season_tb","season_hr","season_bb","season_k"
]

NUMERIC = {"pa","g","ab","slg","season_hits","season_tb","season_hr","season_bb","season_k"}

# Aliases (case-insensitive lookup)
ALIASES = {
    "pa": ["pa","plate_appearances","PA"],
    "ab": ["ab","at_bats","AB"],
    "g":  ["g","games","G"],
    "slg": ["slg","SLG","xslg","XSLG","slugging"],
    "season_hits": ["season_hits","hits","hit","H","total_hits","proj_hits"],
    "season_tb":   ["season_tb","tb","total_bases","b_total_bases","proj_tb"],
    "season_hr":   ["season_hr","home_run","home_runs","hr","HR"],
    "season_bb":   ["season_bb","walk","walks","bb","BB","bases_on_balls"],
    "season_k":    ["season_k","strikeout","strikeouts","k","K","so","SO"],
    "single": ["single","singles","1b","1B"],
    "double": ["double","doubles","2b","2B"],
    "triple": ["triple","triples","3b","3B"],
    "batting_avg": ["batting_avg","avg","AVG"]
}

def fail(msg):
    print(f"❌ normalize_batters_for_builder: {msg}", file=sys.stderr)
    sys.exit(1)

def col_lookup(df_cols, names):
    # Return the first matching column from names (case-insensitive).
    lc_map = {c.lower(): c for c in df_cols}
    for n in names:
        if n.lower() in lc_map:
            return lc_map[n.lower()]
    return None

def ensure_core_identifiers(df):
    for key in ["player_id","name","team"]:
        if key not in df.columns:
            found = col_lookup(df.columns, [key])
            if found:
                df.rename(columns={found: key}, inplace=True)
            else:
                fail(f"Missing identifier column '{key}'")
    return df

def to_numeric_safe(series):
    return pd.to_numeric(series, errors="coerce")

def main():
    if not SRC.exists():
        fail(f"Source file missing: {SRC}")
    df = pd.read_csv(SRC)
    df.columns = df.columns.map(str).str.strip()

    # Ensure id/name/team
    df = ensure_core_identifiers(df)

    # 1) Direct/alias mapping for core numeric fields when available
    for target, names in ALIASES.items():
        if target in {"single","double","triple","batting_avg"}:
            continue
        if target in df.columns:
            continue
        found = col_lookup(df.columns, names)
        if found:
            df.rename(columns={found: target}, inplace=True)

    # 2) Derive 'g' if missing
    if "g" not in df.columns or df["g"].isna().all():
        pa_col = col_lookup(df.columns, ALIASES["pa"])
        if pa_col:
            df["g"] = (to_numeric_safe(df[pa_col]) / 4.2).round().astype("Int64")
        else:
            fail("Cannot derive 'g' (games) — no PA column to base it on.")

    # 3) Try to fill season_* via aliases; if still missing, derive deterministically
    # season_hits
    if "season_hits" not in df.columns or df["season_hits"].isna().all():
        hits_col = col_lookup(df.columns, ALIASES["season_hits"])
        if hits_col:
            df["season_hits"] = to_numeric_safe(df[hits_col])
        else:
            single_col = col_lookup(df.columns, ALIASES["single"])
            double_col = col_lookup(df.columns, ALIASES["double"])
            triple_col = col_lookup(df.columns, ALIASES["triple"])
            hr_col     = col_lookup(df.columns, ALIASES["season_hr"])
            if all(c is not None for c in [single_col,double_col,triple_col,hr_col]):
                df["season_hits"] = (
                    to_numeric_safe(df[single_col]).fillna(0) +
                    to_numeric_safe(df[double_col]).fillna(0) +
                    to_numeric_safe(df[triple_col]).fillna(0) +
                    to_numeric_safe(df[hr_col]).fillna(0)
                )
            else:
                avg_col = col_lookup(df.columns, ALIASES["batting_avg"])
                ab_col  = col_lookup(df.columns, ALIASES["ab"])
                if avg_col and ab_col:
                    df["season_hits"] = (to_numeric_safe(df[avg_col]).fillna(0) * to_numeric_safe(df[ab_col]).fillna(0)).round()
                else:
                    fail("Cannot build 'season_hits': no alias and no reliable fallback (singles/doubles/triples/hr or batting_avg+ab).")

    # season_hr
    if "season_hr" not in df.columns or df["season_hr"].isna().all():
        hr_col = col_lookup(df.columns, ALIASES["season_hr"])
        if hr_col:
            df["season_hr"] = to_numeric_safe(df[hr_col])
        else:
            fail("Cannot map 'season_hr' — add 'home_run'/'hr'.")

    # season_bb
    if "season_bb" not in df.columns or df["season_bb"].isna().all():
        bb_col = col_lookup(df.columns, ALIASES["season_bb"])
        if bb_col:
            df["season_bb"] = to_numeric_safe(df[bb_col])
        else:
            fail("Cannot map 'season_bb' — add 'walk'/'bb'.")

    # season_k
    if "season_k" not in df.columns or df["season_k"].isna().all():
        k_col = col_lookup(df.columns, ALIASES["season_k"])
        if k_col:
            df["season_k"] = to_numeric_safe(df[k_col])
        else:
            fail("Cannot map 'season_k' — add 'strikeout'/'k'.")

    # season_tb
    if "season_tb" not in df.columns or df["season_tb"].isna().all():
        tb_col = col_lookup(df.columns, ALIASES["season_tb"])
        if tb_col:
            df["season_tb"] = to_numeric_safe(df[tb_col])
        else:
            single_col = col_lookup(df.columns, ALIASES["single"])
            double_col = col_lookup(df.columns, ALIASES["double"])
            triple_col = col_lookup(df.columns, ALIASES["triple"])
            hr_col     = col_lookup(df.columns, ALIASES["season_hr"])
            if all(c is not None for c in [single_col,double_col,triple_col,hr_col]):
                df["season_tb"] = (
                    1*to_numeric_safe(df[single_col]).fillna(0) +
                    2*to_numeric_safe(df[double_col]).fillna(0) +
                    3*to_numeric_safe(df[triple_col]).fillna(0) +
                    4*to_numeric_safe(df[hr_col]).fillna(0)
                )
            else:
                slg_col = col_lookup(df.columns, ALIASES["slg"])
                ab_col  = col_lookup(df.columns, ALIASES["ab"])
                if slg_col and ab_col:
                    df["season_tb"] = (to_numeric_safe(df[slg_col]).fillna(0) * to_numeric_safe(df[ab_col]).fillna(0))
                else:
                    fail("Cannot build 'season_tb': no alias and no reliable fallback (components or slg*ab).")

    # 4) Fill pa/ab/slg if missing
    if "pa" not in df.columns or df["pa"].isna().all():
        pa_col = col_lookup(df.columns, ALIASES["pa"])
        if pa_col:
            df["pa"] = to_numeric_safe(df[pa_col])
        else:
            fail("Missing 'pa' and no alias.")

    if "ab" not in df.columns or df["ab"].isna().all():
        ab_col = col_lookup(df.columns, ALIASES["ab"])
        if ab_col:
            df["ab"] = to_numeric_safe(df[ab_col])
        else:
            fail("Missing 'ab' and no alias.")

    if "slg" not in df.columns or df["slg"].isna().all():
        df["slg"] = np.where(to_numeric_safe(df["ab"]).fillna(0) > 0,
                             to_numeric_safe(df["season_tb"]) / to_numeric_safe(df["ab"]),
                             0.0)

    # Final coercion & validation
    for col in NUMERIC:
        df[col] = to_numeric_safe(df[col])

    before = len(df)
    df = df.dropna(subset=list(NUMERIC), how="any").copy()
    dropped = before - len(df)

    df = df[(df["g"] > 0) & (df["pa"] >= 0) & (df["ab"] >= 0)]
    df["slg"] = df["slg"].clip(lower=0.0, upper=2.0)

    if df.empty:
        fail("All rows invalid after normalization; check inputs.")

    df.to_csv(SRC, index=False)
    print(f"✅ normalize_batters_for_builder: wrote {SRC}  (kept {len(df)}/{before}, dropped {dropped})")

if __name__ == "__main__":
    main()
