import os
import pandas as pd
import unicodedata
import re

csv_folder = "data/team_csvs"
output_file = "data/processed/player_team_master.csv"

rows = []

# --- Start of updated normalization functions ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in text if unicodedata.category(c) != 'Mn')

def _capitalize_mc_names_in_string(text):
    """
    Specifically targets words starting with 'Mc' or 'mc' and
    ensures the letter immediately following 'Mc' is capitalized.
    E.g., 'mccullers' -> 'McCullers', 'Mcgregor' -> 'McGregor'.
    """
    def replacer(match):
        prefix = match.group(1) # 'Mc' or 'mc'
        char_to_capitalize = match.group(2).upper() # The letter after 'Mc'
        rest_of_name = match.group(3).lower() # The rest of the word in lowercase
        return prefix.capitalize() + char_to_capitalize + rest_of_name

    text = re.sub(r"\b(mc)([a-z])([a-z]*)\b", replacer, text, flags=re.IGNORECASE)
    return text

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name) # Remove non-word, non-space, non-comma, non-dot chars
    name = re.sub(r"\s+", " ", name).strip() # Consolidate spaces

    # Step 1: Apply general title casing to the entire cleaned string.
    temp_name = name.title() 

    # Step 2: Apply the specific 'Mc' capitalization fix.
    final_normalized_name = _capitalize_mc_names_in_string(temp_name)

    # Step 3: Handle "Last, First" vs "First Last" formatting.
    if "," in final_normalized_name:
        parts = [p.strip() for p in final_normalized_name.split(",")]
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}"
        return ' '.join(parts)
    else:
        tokens = final_normalized_name.split()
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return ' '.join(tokens)
# --- End of updated normalization functions ---


for filename in os.listdir(csv_folder):
    file_path = os.path.join(csv_folder, filename)
    if not filename.endswith(".csv"):
        continue

    if filename.startswith("batters_"):
        team = filename.replace("batters_", "").replace(".csv", "")
        df = pd.read_csv(file_path)
        if "name" in df.columns:
            for name in df["name"].dropna():
                normalized = normalize_name(name)
                rows.append({"name": normalized, "team": team, "type": "batter"})

    elif filename.startswith("pitchers_"):
        team = filename.replace("pitchers_", "").replace(".csv", "")
        df = pd.read_csv(file_path)
        if "last_name, first_name" in df.columns:
            for name in df["last_name, first_name"].dropna():
                normalized = normalize_name(name)
                rows.append({"name": normalized, "team": team, "type": "pitcher"})

master_df = pd.DataFrame(rows)
master_df = master_df.sort_values(["team", "type", "name"])
os.makedirs("data/processed", exist_ok=True)
master_df.to_csv(output_file, index=False)
