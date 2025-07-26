import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/raw/todaysgames.csv")
OUTPUT_FILE = Path("data/raw/todaysgames.csv")  # Overwrites original

def flip_name(name):
    if not isinstance(name, str) or name.strip() == "":
        return name
    tokens = name.strip().split()
    if len(tokens) < 2:
        return name  # skip if name can't be split
    first = " ".join(tokens[:-1])
    last = tokens[-1]
    return f"{last}, {first}"

def main():
    df = pd.read_csv(INPUT_FILE)
    df["pitcher_home"] = df["pitcher_home"].apply(flip_name)
    df["pitcher_away"] = df["pitcher_away"].apply(flip_name)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Converted pitcher names to 'Last, First' format in {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
