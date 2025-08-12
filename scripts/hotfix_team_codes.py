#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/todaysgames_normalized.csv")
BACKUP = Path("data/raw/todaysgames_normalized.bak.csv")

# Extend/adjust this mapping as needed
TEAM_FIXES = {
    "AZ": "Diamondbacks",
    "Az": "Diamondbacks",
    "az": "Diamondbacks",
    "CWS": "WhiteSox",
    "Cws": "WhiteSox",
    "cws": "WhiteSox",
}

def main():
    if not INPUT.exists():
        print(f"❌ Not found: {INPUT}")
        return

    df = pd.read_csv(INPUT)
    changed = 0

    for col in ("home_team", "away_team"):
        if col not in df.columns:
            print(f"ℹ️ Column '{col}' not in {INPUT.name}; skipping that column.")
            continue

        # Normalize to string for safe replace
        before = df[col].astype(str).copy()
        df[col] = before.replace(TEAM_FIXES)

        # Count changes
        col_changes = (before != df[col]).sum()
        changed += col_changes
        print(f"🔧 {col}: fixed {col_changes} value(s).")

    if changed == 0:
        print("✅ No team code fixes required—file already normalized.")
        return

    # Make a one-time backup on first change
    try:
        if not BACKUP.exists():
            df_original = pd.read_csv(INPUT)
            df_original.to_csv(BACKUP, index=False)
            print(f"🧷 Backup written: {BACKUP}")
    except Exception as e:
        print(f"⚠️ Could not write backup: {e}")

    df.to_csv(INPUT, index=False)
    print(f"✅ Saved normalized teams to {INPUT}")

if __name__ == "__main__":
    main()
