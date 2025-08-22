#!/usr/bin/env python3
"""
Append today's batter props from data/bets/prep/batter_props_final.csv
into data/bets/player_props_history.csv, mapping columns to history schema.
"""

from pathlib import Path
import pandas as pd
import datetime as dt

PREP_FILE = Path("data/bets/prep/batter_props_final.csv")
HISTORY_FILE = Path("data/bets/player_props_history.csv")

HISTORY_COLUMNS = [
    "player_id","name","team","prop","line","value",
    "over_probability","date","game_id","prop_correct","prop_sort"
]

def main():
    if not PREP_FILE.exists():
        print(f"❌ Missing prep file: {PREP_FILE}")
        return

    df = pd.read_csv(PREP_FILE)
    if df.empty:
        print("❌ Prep file is empty")
        return

    # normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # try to map common fields
    out = pd.DataFrame()
    out["player_id"] = df.get("player_id")
    out["name"] = df.get("player_name") or df.get("name")
    out["team"] = df.get("team")
    out["prop"] = df.get("prop_type") or df.get("prop")
    out["line"] = df.get("prop_line") or df.get("line")
    out["value"] = df.get("value") or df.get("odds")
    out["over_probability"] = df.get("over_probability") or pd.NA
    out["date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.date.astype(str)
    out["game_id"] = df.get("game_id")
    out["prop_correct"] = pd.NA
    out["prop_sort"] = df.get("prop_sort") or pd.NA

    # only keep today's rows
    today = str(dt.date.today())
    out = out[out["date"] == today]

    if out.empty:
        print("❌ No rows for today in prep file")
        return

    # union with existing history if present
    if HISTORY_FILE.exists():
        hist = pd.read_csv(HISTORY_FILE)
        combined = pd.concat([hist, out], ignore_index=True)
    else:
        combined = out

    # enforce schema and dedupe
    for col in HISTORY_COLUMNS:
        if col not in combined.columns:
            combined[col] = pd.NA
    combined = combined[HISTORY_COLUMNS]
    combined = combined.drop_duplicates(
        subset=["player_id","prop","line","date","game_id"], keep="last"
    )

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HISTORY_FILE, index=False)
    print(f"✅ Appended {len(out)} rows into {HISTORY_FILE}; total {len(combined)} rows now")

if __name__ == "__main__":
    main()
