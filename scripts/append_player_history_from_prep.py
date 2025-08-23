
# scripts/append_player_history_from_prep.py
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


PREP_FILE = Path("data/bets/prep/batter_props_final.csv")
PROJ_FILE = Path("data/_projections/batter_props_projected.csv")
OUT_FILE  = Path("data/bets/player_props_history.csv")

# Mapping from (prop, line) -> projection column
PROP_PROJ_COL = {
    ("hits", 1.5): "prob_hits_over_1p5",
    ("total_bases", 1.5): "prob_tb_over_1p5",
    ("hr", 0.5): "prob_hr_over_0p5",
}

PROP_SORT = {"hits": 10.0, "total_bases": 20.0, "hr": 30.0}


def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _read_csv_safe(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists():
        return None
    try:
        return _std(pd.read_csv(p))
    except Exception:
        return None


def _normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # player_id → Int64 (nullable)
    if "player_id" in df.columns:
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")

    # game_id → string without trailing .0 (nullable)
    if "game_id" in df.columns:
        gid = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
        df["game_id"] = gid.astype("string")  # leaves <NA> as proper missing sentinel

    # line → float
    if "line" in df.columns:
        df["line"] = pd.to_numeric(df["line"], errors="coerce")

    # prop lowercase & trimmed
    if "prop" in df.columns:
        df["prop"] = df["prop"].astype(str).str.strip().str.lower()

    # date → date (yyyy-mm-dd), kept as string for CSV compatibility
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["date"] = dt.astype("string")

    return df


def _choose_proj_col(prop: str, line: float) -> Optional[str]:
    key = (prop, float(line) if pd.notna(line) else None)
    return PROP_PROJ_COL.get(key)


def _merge_probs(prep: pd.DataFrame, proj: Optional[pd.DataFrame]) -> pd.DataFrame:
    df = prep.copy()

    if proj is None or proj.empty:
        return df

    proj = _std(proj)

    # Normalize IDs in projections too
    if "player_id" in proj.columns:
        proj["player_id"] = pd.to_numeric(proj["player_id"], errors="coerce").astype("Int64")

    # Join once on player_id to bring in all prob_* columns
    df["proj_prob"] = pd.NA
    join_cols = [c for c in ["player_id"] if c in df.columns and c in proj.columns]
    if join_cols:
        keep_cols = ["player_id"] + [c for c in proj.columns if c.startswith("prob_")]
        merged = df.merge(
            proj[keep_cols].drop_duplicates("player_id"),
            on="player_id",
            how="left",
            validate="m:1",
        )
    else:
        merged = df.copy()

    def row_prob(row):
        col = _choose_proj_col(row.get("prop"), row.get("line"))
        if col and col in merged.columns:
            return row.get(col)
        return pd.NA

    merged["proj_prob"] = merged.apply(row_prob, axis=1)

    # Prefer projection probability when present
    if "over_probability" in merged.columns:
        merged["over_probability"] = pd.to_numeric(merged["over_probability"], errors="coerce")

    merged["over_probability"] = merged["proj_prob"].where(
        merged["proj_prob"].notna(), merged.get("over_probability", pd.NA)
    )

    # Do NOT clip or jitter anymore. Keep the raw numbers.
    merged.drop(columns=[c for c in ["proj_prob"] if c in merged.columns], inplace=True)
    return merged


def _ensure_prop_sort(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "prop_sort" not in df.columns:
        df["prop_sort"] = pd.NA
    mask = df["prop_sort"].isna()
    df.loc[mask, "prop_sort"] = df.loc[mask, "prop"].map(PROP_SORT).astype("Float64")
    return df


def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Filter out rows with missing game_id only if column exists
    if "game_id" in df.columns:
        df = df[df["game_id"].notna()].copy()

    # Preferred de-dup key (include game_id if present; otherwise fall back)
    preferred = ["date", "game_id", "player_id", "team", "prop", "line"]
    keys = [k for k in preferred if k in df.columns]
    if not keys:
        return df

    # Keep the latest record when multiple exist
    order_cols = [c for c in ("asof", "timestamp", "created_at", "date") if c in df.columns]
    if order_cols:
        df = df.sort_values(by=order_cols)

    return df.drop_duplicates(subset=keys, keep="last")


def main():
    prep = _read_csv_safe(PREP_FILE)
    if prep is None or prep.empty:
        print(f"⚠️ {PREP_FILE} is missing or empty; nothing to append.")
        return

    prep = _normalize_ids(prep)
    proj = _read_csv_safe(PROJ_FILE)

    # Merge/repair probabilities
    prep = _merge_probs(prep, proj)

    # prop_sort fill
    prep = _ensure_prop_sort(prep)

    # Keep only columns we know about (and preserve any extras if present)
    base_cols = [
        "player_id","name","team","prop","line","value",
        "over_probability","date","game_id","prop_correct","prop_sort"
    ]
    cols = [c for c in base_cols if c in prep.columns] + [c for c in prep.columns if c not in base_cols]
    prep = prep[cols]

    # De-duplicate within today's prep
    prep = _dedupe(prep)

    # Append to history
    if OUT_FILE.exists():
        hist = _read_csv_safe(OUT_FILE)
        if hist is None:
            hist = pd.DataFrame(columns=prep.columns)
    else:
        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        hist = pd.DataFrame(columns=prep.columns)

    combo = pd.concat([hist, prep], ignore_index=True)
    combo = _normalize_ids(combo)
    combo = _ensure_prop_sort(combo)
    combo = _dedupe(combo)

    combo.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(combo)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
