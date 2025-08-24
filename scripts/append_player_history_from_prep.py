# scripts/append_player_history_from_prep.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np


# ---------- IO ----------
def _read_csv_any(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Prep CSV not found at '{path}'. Provide the correct path via --prep-csv.")
    return pd.read_csv(p)


def _write_csv(df: pd.DataFrame, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)


# ---------- Normalization ----------
ALIASES = {
    # target -> possible source names (lowercased/stripped)
    "player_id": {"player_id", "playerid", "mlb_id", "bat_mlbid", "id"},
    "name": {"name", "player", "player_name"},
    "team": {"team", "tm"},
    "prop": {"prop", "market", "stat"},
    "over_probability": {"over_probability", "prob_over", "over_prob", "over", "p_over"},
    "date": {"date", "game_date"},
}

REQUIRED_MIN = {"player_id", "name", "team", "prop", "over_probability"}  # 'date' is filled if missing


def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    low = {c.lower().strip(): c for c in df.columns}

    def pick_one(canon: str) -> str | None:
        for alias in ALIASES[canon]:
            if alias in low:
                return low[alias]
        return None

    rename = {}
    for tgt in ["player_id", "name", "team", "prop", "over_probability", "date"]:
        src = pick_one(tgt)
        if src and src != tgt:
            rename[src] = tgt
    if rename:
        df = df.rename(columns=rename)

    # ensure required types
    if "over_probability" in df.columns:
        df["over_probability"] = pd.to_numeric(df["over_probability"], errors="coerce")

    return df


def _ensure_min_columns(df: pd.DataFrame, today_str: str) -> pd.DataFrame:
    df = df.copy()

    # Fill/format date
    if "date" not in df.columns:
        df["date"] = today_str
    else:
        # coerce any datetime-like into YYYY-MM-DD
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
        except Exception:
            df["date"] = df["date"].astype("string")
        df["date"] = df["date"].fillna(today_str)
        # normalize like 2025-08-24
        df["date"] = df["date"].astype(str).str[:10]

    missing = [c for c in REQUIRED_MIN if c not in df.columns]
    if missing:
        raise ValueError(
            "Prep file is missing required column(s): "
            f"{missing}. At minimum, the prep CSV must include: {sorted(REQUIRED_MIN)} "
            "(date is auto-filled if absent)."
        )

    # clean basic fields
    for c in ["player_id", "name", "team", "prop"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # probability safety
    df["over_probability"] = df["over_probability"].clip(lower=0.0, upper=1.0)

    return df


# ---------- Main ----------
def run(prep_csv: str | Path, out_csv: str | Path) -> None:
    today_str = date.today().isoformat()

    df = _read_csv_any(prep_csv)
    df = _std_cols(df)
    df = _ensure_min_columns(df, today_str)

    # keep only essential columns plus anything else you already had
    cols = ["date", "player_id", "name", "team", "prop", "over_probability"]
    keep = [c for c in cols if c in df.columns]
    base = df[keep].copy()

    # append/deduplicate
    p_out = Path(out_csv)
    if p_out.exists():
        hist = pd.read_csv(p_out)
        # standardize history too
        hist = _std_cols(hist)
        hist = _ensure_min_columns(hist, today_str)
        base = pd.concat([hist, base], ignore_index=True)

    # de-dupe on unique key (date, player_id, prop) keep last
    base = (
        base.sort_index()
        .drop_duplicates(subset=["date", "player_id", "prop"], keep="last")
        .reset_index(drop=True)
    )

    _write_csv(base, p_out)
    print(f"✅ Appended {len(df)} rows (after merge: {len(base)}) -> {p_out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Append player history from prep CSV.")
    ap.add_argument("--prep-csv", default="data/batter_props_final.csv", help="Path to the prepared daily props CSV.")
    ap.add_argument("--out-csv", default="data/player_history.csv", help="Path to the accumulated player history CSV.")
    args = ap.parse_args()

    try:
        run(prep_csv=args.prep_csv, out_csv=args.out_csv)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
