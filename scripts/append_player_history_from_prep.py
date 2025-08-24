# scripts/append_player_history_from_prep.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
from datetime import date

IN_PREP_DEFAULT = Path("data/batter_props_final.csv")
OUT_HISTORY = Path("data/player_props_history.csv")
TODAY = pd.Timestamp(date.today()).normalize().date()

REQUIRED_MIN = ["date","player_id","name","team","prop","over_probability"]

def _read_csv_any(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"Prep CSV not found at '{p}'.")
    df = pd.read_csv(p)
    df.columns = df.columns.str.strip()
    return df

def _ensure_min_cols(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_MIN if c not in df.columns]
    if missing:
        raise ValueError(f"Prep file is missing required column(s): {missing}. "
                         f"At minimum it must include {REQUIRED_MIN}.")
    return df

def _ensure_date_today(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns or df["date"].isna().all():
        df = df.copy()
        df["date"] = TODAY
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def run(prep_csv: Path = IN_PREP_DEFAULT, out_csv: Path = OUT_HISTORY) -> None:
    df = _read_csv_any(prep_csv)
    df = _ensure_date_today(df)
    df = _ensure_min_cols(df)

    # Keep only today's rows
    df = df[df["date"] == TODAY].copy()

    if df.empty:
        print("ℹ️  No rows for today — nothing to append.")
        return

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if out_csv.exists():
        # Append without dupes (player_id + prop + date)
        hist = pd.read_csv(out_csv)
        hist.columns = hist.columns.str.strip()
        hist["date"] = pd.to_datetime(hist["date"]).dt.date
        before = len(hist)
        key_cols = ["date","player_id","prop"]
        combined = pd.concat([hist, df], ignore_index=True)
        combined.drop_duplicates(subset=key_cols, keep="last", inplace=True)
        combined.to_csv(out_csv, index=False)
        print(f"✅ Appended {len(combined)-before} new rows → {out_csv}")
    else:
        df.to_csv(out_csv, index=False)
        print(f"✅ Created history with {len(df)} rows → {out_csv}")

def main():
    run()

if __name__ == "__main__":
    main()
