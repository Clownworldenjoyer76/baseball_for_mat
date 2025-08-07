import pandas as pd
from pathlib import Path

# === CONFIG ===
INPUT_DIR = Path("data/rosters")
OUTPUT_DIR = Path("data/rosters/ready")  # Updated output directory

# === HELPERS ===
def normalize_name(full_name):
    parts = full_name.strip().split()
    if len(parts) < 2:
        return full_name.strip()
    return f"{' '.join(parts[1:])}, {parts[0]}"

# === MAIN ===
for file in INPUT_DIR.glob("*_roster.csv"):
    team_name = file.stem.replace("_roster", "")
    df = pd.read_csv(file)

    # Normalize fields
    df["team"] = team_name
    df["last_name, first_name"] = df["name"].apply(normalize_name)
    df["player_id"] = df["id"]

    # Filter + select final columns
    batters = df[df["position_code"] != 1][["team", "last_name, first_name", "player_id"]]
    pitchers = df[df["position_code"] == 1][["team", "last_name, first_name", "player_id"]]

    # Write output files
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    batters.to_csv(OUTPUT_DIR / f"b_{team_name}.csv", index=False)
    pitchers.to_csv(OUTPUT_DIR / f"p_{team_name}.csv", index=False)
