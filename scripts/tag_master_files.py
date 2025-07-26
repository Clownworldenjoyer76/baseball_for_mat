import pandas as pd
import os
import unicodedata
import re
from datetime import datetime

SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    tokens = name.replace(",", "").split()
    if len(tokens) >= 2:
        if tokens[-1].lower().strip(".") in SUFFIXES and len(tokens) >= 3:
            last = f"{tokens[-2]} {tokens[-1]}"
            first = " ".join(tokens[:-2])
        else:
            last = tokens[-1]
            first = " ".join(tokens[:-1])
        return f"{last.strip().title()}, {first.strip().title()}"
    return name.title()

master_df = pd.read_csv("data/processed/player_team_master.csv")
batter_file = "data/normalized/batters_normalized.csv"
pitcher_file = "data/normalized/pitchers_normalized.csv"
output_folder = "data/tagged"
output_totals_file = "data/output/player_totals.txt"

os.makedirs(output_folder, exist_ok=True)
os.makedirs("data/output", exist_ok=True)

master_df["name"] = master_df["name"].apply(normalize_name)

def tag_players(file_path, player_type):
    df = pd.read_csv(file_path)
    if "last_name, first_name" not in df.columns:
        print(f"❌ Column 'last_name, first_name' not found in {file_path}")
        return pd.DataFrame()

    df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)

    merged = df.merge(
        master_df,
        how="left",
        left_on="last_name, first_name",
        right_on="name",
        suffixes=("", "_master")
    )

    unmatched = merged[merged["team"].isna() | merged["type"].isna()]
    if not unmatched.empty:
        print(f"⚠️ {len(unmatched)} {player_type} rows had no team/type match and will be dropped:")
        print(unmatched[["last_name, first_name"]].drop_duplicates().to_string(index=False))

    merged_clean = merged.dropna(subset=["team", "type"])

    key_cols = ["name", "player_id", "team", "type"]
    other_cols = [col for col in merged_clean.columns if col not in key_cols]
    merged_clean = merged_clean[key_cols + other_cols]

    output_file = os.path.join(output_folder, os.path.basename(file_path))
    merged_clean.to_csv(output_file, index=False)
    print(f"✅ Tagged {player_type}: {output_file} ({len(merged_clean)} rows)")

    return merged_clean

batters_tagged = tag_players(batter_file, "batters")
pitchers_tagged = tag_players(pitcher_file, "pitchers")

with open(output_totals_file, "w") as f:
    f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Tagged Batters: {len(batters_tagged)}\n")
    f.write(f"Tagged Pitchers: {len(pitchers_tagged)}\n")
