# scripts/append_player_history_from_prep.py
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


# ---------- Paths ----------
SRC_PATH = Path("data/_projections/batter_props_projected.csv")
HIST_PATH = Path("data/history/player_props_history.csv")


def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.map(str).str.strip()
    return df


def _read_csv_safe(p: Path) -> pd.DataFrame | None:
    if not p.exists():
        return None
    try:
        return _std(pd.read_csv(p))
    except Exception as e:
        print(f"Could not read {p}: {e}", file=sys.stderr)
        return None


def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "date" not in df.columns or df["date"].isna().all():
        # Fill with today's date in YYYY-MM-DD
        df["date"] = datetime.now().astimezone().date().isoformat()
    # Normalize to YYYY-MM-DD if strings like 2025/08/23 appear
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    return df


def _ensure_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "timestamp" not in df.columns or df["timestamp"].isna().all():
        df["timestamp"] = datetime.now().astimezone().isoformat()
    return df


def _dedupe_on_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    df = df.copy()
    # Keep the last occurrence (newest write) for each key set
    return df.drop_duplicates(subset=keys, keep="last")


def main() -> int:
    # Load source
    src = _read_csv_safe(SRC_PATH)
    if src is None or src.empty:
        print(f"Source not found or empty: {SRC_PATH}", file=sys.stderr)
        return 1

    # Standardize/ensure required cols
    src = _ensure_date_column(src)
    src = _ensure_timestamp(src)

    # Keys for dedupe into history
    keys = []
    for k in ["player_id", "date"]:
        if k in src.columns:
            keys.append(k)
    # Fallback if player_id is missing, use name+team+date
    if not keys:
        alt = [k for k in ["name", "team", "date"] if k in src.columns]
        if alt:
            keys = alt
        else:
            # If absolutely nothing to key on, create a synthetic row_id
            src = src.copy()
            src["row_id"] = range(len(src))
            keys = ["row_id"]

    # Load existing history (if any)
    hist = _read_csv_safe(HIST_PATH)

    if hist is None or hist.empty:
        # First write: just ensure directories and write src
        HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        out = _dedupe_on_keys(src, keys)
        out.to_csv(HIST_PATH, index=False)
        print(f"Wrote {len(out)} rows to new history file -> {HIST_PATH}")
        return 0

    # Align columns: union of cols, fill missing with NA
    all_cols = list(dict.fromkeys(list(hist.columns) + list(src.columns)))
    hist_u = hist.reindex(columns=all_cols)
    src_u = src.reindex(columns=all_cols)

    # Append and dedupe on keys
    combined = pd.concat([hist_u, src_u], ignore_index=True)
    combined = _dedupe_on_keys(combined, keys)

    # Sort for readability: by date then player_id/name if present
    sort_cols = [c for c in ["date", "player_id", "name", "team"] if c in combined.columns]
    if sort_cols:
        combined = combined.sort_values(sort_cols).reset_index(drop=True)

    # Overwrite history file
    HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HIST_PATH, index=False)
    print(f"Appended {len(src)} rows, wrote {len(combined)} total rows -> {HIST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
