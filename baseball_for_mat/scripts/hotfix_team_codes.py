#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/todaysgames_normalized.csv")
OUTPUT = Path("data/raw/todaysgames_normalized.csv")

def main():
    if not INPUT.exists():
        print(f"⚠️ {INPUT} not found, skipping.")
        return

    df = pd.read_csv(INPUT)

    # Hotfix known code issues
    df["home_team"] = df["home_team"].replace({
        "CHW": "CWS",  # White Sox
        "AZ": "ARI",   # Diamondbacks
        "OAKL": "OAK"  # Athletics variations
    })
    df["away_team"] = df["away_team"].replace({
        "CHW": "CWS",
        "AZ": "ARI",
        "OAKL": "OAK"
    })

    df.to_csv(OUTPUT, index=False)
    print(f"✅ hotfix_team_codes applied fixes -> {OUTPUT}")

if __name__ == "__main__":
    main()
