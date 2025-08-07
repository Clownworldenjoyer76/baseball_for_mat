import os
from pathlib import Path
import shutil

# === CONFIG ===
INPUT_DIR = Path("data/rosters/ready")
OUTPUT_DIR = Path("data/team_csvs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# === MAPPINGS ===
TWO_WORD_TEAMS = {
    "Red_Sox": "RedSox",
    "White_Sox": "WhiteSox",
    "Blue_Jays": "BlueJays"
}

TWO_WORD_CITIES = {
    "Kansas_City",
    "Los_Angeles",
    "New_York",
    "San_Diego",
    "San_Francisco",
    "St._Louis",
    "Tampa_Bay"
}

NO_CITY_TEAMS = {
    "Athletics"
}

# === MAIN ===
for file in INPUT_DIR.glob("*.csv"):
    stem = file.stem  # e.g., "b_Boston_Red_Sox"
    
    if stem.startswith("b_"):
        role = "batters"
        name_part = stem[2:]
    elif stem.startswith("p_"):
        role = "pitchers"
        name_part = stem[2:]
    else:
        continue  # skip any unexpected files

    tokens = name_part.split("_")

    # Handle known no-city teams (e.g., Athletics.csv)
    if name_part in NO_CITY_TEAMS:
        final_name = name_part
    else:
        # Try to remove known 2-word city name
        possible_city = "_".join(tokens[:2])
        if possible_city in TWO_WORD_CITIES:
            team_tokens = tokens[2:]
        else:
            team_tokens = tokens[1:]

        # Reconstruct team name
        team_key = "_".join(team_tokens)
        final_name = TWO_WORD_TEAMS.get(team_key, "".join(team_tokens))

    # Final output filename
    output_filename = f"{role}_{final_name}.csv"
    output_path = OUTPUT_DIR / output_filename

    # Copy to destination
    shutil.copyfile(file, output_path)
