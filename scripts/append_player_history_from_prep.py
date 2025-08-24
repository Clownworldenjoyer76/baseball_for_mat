# scripts/append_player_history_from_prep.py
from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Optional, Sequence

import numpy as np
import pandas as pd


# ---- Defaults (fixed prep path) ----
DEFAULT_PREP: Path = Path("data/bets/prep/batter_props_final.csv")
DEFAULT_OUT: Path = Path("data/player_history.csv")  # change if your repo uses a different target


# ---- Utilities ----
def _read_csv_any(path: Path) -> pd.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"Prep CSV not found at '{path}'. Provide the correct path via --prep-csv.")
    return pd.read_csv(path)

def _ensure_columns(df: pd.DataFrame, required: Sequence[str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            "Prep file is missing required column(s): "
            f"{missing}. At minimum, the prep CSV must include these columns: {list(required)}."
        )
    return df

def _today_str() -> str:
    # Keep it simple (UTC date is fine for consistency in CI);
    # if you need US/Eastern, wire it in here.
    return date.today().isoformat()

def _coerce_date_col(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "date" not in df.columns:
        # backfill with today if missing entirely
        df["date"] = _today_str()

    # try to coerce to yyyy-mm-dd strings
    def _to_str(d):
        try:
            return pd.to_datetime(d).date().isoformat()
        except Exception:
            return _today_str()

    df["date"] = df["date"].apply(_to_str)
    return df

def _limit_top5_per_player(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep at most 5 props per player_id for today's games.
    Priority for sorting:
        1) 'value' (desc) if present
        2) 'over_probability' (desc) if present
        3) 'mega_z' or 'batter_z' (desc) as last resort
    """
    sort_keys: Iterable[str] = []
    for c in ("value", "over_probability", "mega_z", "batter_z"):
        if c in df.columns:
            sort_keys.append(c)

    if sort_keys:
        df = df.sort_values(list(sort_keys), ascending=[False] * len(sort_keys))
    else:
        # No ranking signal at all — keep stable order but still limit
        pass

    if "player_id" not in df.columns:
        # fallback: group by name+team if player_id isn’t present
        grp_cols = [c for c in ["name", "team"] if c in df.columns]
    else:
        grp_cols = ["player_id"]

    df = df.groupby(grp_cols, as_index=False).head(5)
    return df.reset_index(drop=True)


# ---- Main pipeline ----
def run(prep_csv: Path, out_csv: Path) -> None:
    # Required columns for downstream
    required_cols = ("date", "player_id", "name", "team", "prop", "over_probability")

    df = _read_csv_any(prep_csv)
    df = _coerce_date_col(df)

    # Filter to ONLY today's games
    today = _today_str()
    df = df[df["date"].astype(str) == today].copy()

    # Validate required columns (after we possibly added 'date')
    df = _ensure_columns(df, required_cols)

    # Enforce max 5 props per player
    df = _limit_top5_per_player(df)

    # Overwrite the target file (no appending — per your requirement)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)

    print(f"✅ Wrote {len(df)} rows for {today} -> {out_csv}")
    kept = (
        df.groupby("player_id", dropna=False)
          .size()
          .rename("props_kept")
          .reset_index()
          .sort_values("props_kept", ascending=False)
          .head(5)
    )
    print("Top players by props kept (max 5):")
    print(kept.to_string(index=False))


def main():
    p = argparse.ArgumentParser(description="Normalize and cap batter props to 5 per player for today's games.")
    p.add_argument("--prep-csv", type=Path, default=DEFAULT_PREP,
                   help="Path to prep CSV (default: data/bets/prep/batter_props_final.csv)")
    p.add_argument("--out-csv", type=Path, default=DEFAULT_OUT,
                   help="Where to write the normalized file (OVERWRITES).")
    args = p.parse_args()

    run(prep_csv=args.prep_csv, out_csv=args.out_csv)


if __name__ == "__main__":
    main()
