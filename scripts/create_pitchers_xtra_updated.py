import pandas as pd
from pathlib import Path
import unicodedata
import re

# Define paths
INPUT_FILE = Path("data/end_chain/cleaned/games_cleaned.csv")
REF_FILE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
OUTPUT_FILE = Path("data/end_chain/cleaned/pitchers_xtra_updated.csv")

# Columns for output file
OUTPUT_COLUMNS = [
    "pitcher_home", "pitcher_away", "team", "name",
    "innings_pitched", "strikeouts", "walks", "earned_runs"
]

def strip_accents(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def capitalize_mc_names(text):
    return re.sub(r'\b(mc)([a-z])([a-z]*)\b',
                  lambda m: m.group(1).capitalize() + m.group(2).upper() + m.group(3).lower(),
                  text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip().rstrip(',')
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = capitalize_mc_names(name)

    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    tokens = name.split()

    if len(tokens) >= 2:
        first = tokens[0]
        possible_suffix = tokens[-1].lower().strip(".")
        if possible_suffix in suffixes and len(tokens) > 2:
            last = " ".join(tokens[1:-1])
            suffix = tokens[-1]
            return f"{last} {suffix}, {first}"
        else:
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
    return name.title()

def main():
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        return
    if not REF_FILE.exists():
        print(f"❌ Reference file not found: {REF_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    ref = pd.read_csv(REF_FILE)

    required_cols = {"pitcher_home", "pitcher_away"}
    if not required_cols.issubset(df.columns):
        print(f"❌ Input file missing required columns: {required_cols - set(df.columns)}")
        return

    # Create output DataFrame with blanks
    output_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    output_df["pitcher_home"] = df["pitcher_home"]
    output_df["pitcher_away"] = df["pitcher_away"]

    for col in ["innings_pitched", "strikeouts", "walks", "earned_runs"]:
        output_df[col] = ""

    # Normalize name from pitcher_home
    output_df["name"] = output_df["pitcher_home"].astype(str).apply(normalize_name)

    # Normalize name in reference file
    ref["name"] = ref["last_name, first_name"].astype(str).apply(normalize_name)
    ref_team_lookup = ref[["name", "team"]]

    # Merge team into output_df
    output_df = output_df.merge(ref_team_lookup, on="name", how="left")

    # Debug info if team is missing
    missing_teams = output_df["team"].isna().sum()
    print(f"🔎 Missing team for {missing_teams} rows")

    # Save final file
    output_df = output_df[OUTPUT_COLUMNS]  # enforce correct column order
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Created {OUTPUT_FILE} with shape {output_df.shape}")

if __name__ == "__main__":
    main()
