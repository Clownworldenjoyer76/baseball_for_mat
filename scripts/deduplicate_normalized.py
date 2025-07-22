import os
import pandas as pd
import unicodedata
import re
import subprocess

# --- Normalization Functions ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in text if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""

    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)  # Keep alphanumerics, comma, period
    name = re.sub(r"\s+", " ", name).strip()

    if "," not in name:
        tokens = name.split()
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return name.title()

    parts = name.split(",")
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().title()
        return f"{last}, {first}"

    return name.title()

# --- Main Deduplication Logic ---
files = {
    "batters": "data/tagged/batters_normalized.csv",
    "pitchers": "data/tagged/pitchers_normalized.csv"
}
output_dir = "data/cleaned"
os.makedirs(output_dir, exist_ok=True)

for label, path in files.items():
    if os.path.exists(path):
        df = pd.read_csv(path)
        before = len(df)

        # Normalize names before deduplication
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
        df = df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
        after = len(df)

        print(f"🧼 {label.capitalize()} deduplicated: {before} → {after}")

        # Map team names to official casing using team_name_master
        try:
            team_map = pd.read_csv("data/Data/team_name_master.csv")
            team_map = team_map[['team_name', 'clean_team_name']].dropna()
            reverse_map = dict(zip(team_map['clean_team_name'].str.strip(), team_map['team_name'].str.strip()))
            df['team'] = df['team'].astype(str).str.strip().map(reverse_map)
            print(f"🔗 {label.capitalize()} team names mapped using team_name_master.csv")
        except Exception as e:
            print(f"⚠️ Failed to map team names for {label}: {e}")

        output_path = f"{output_dir}/{label}_normalized_cleaned.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Wrote cleaned {label} data to {output_path}")

        # Git commit for output file
        try:
            subprocess.run(["git", "add", output_path], check=True)
            subprocess.run(["git", "commit", "-m", f"🧹 Auto-cleaned and deduplicated {label}"], check=True)
            subprocess.run(["git", "push"], check=True)
            print(f"✅ Git commit and push completed for {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Git commit/push failed for {output_path}: {e}")
