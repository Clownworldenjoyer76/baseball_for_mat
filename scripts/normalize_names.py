import pandas as pd
import unicodedata
import re
import os # Import os for path checking

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUT_HOME = "data/adjusted/pitchers_home.csv"
OUT_AWAY = "data/adjusted/pitchers_away.csv"

# --- Normalization Utilities ---
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

# --- Main Logic ---
def load_games():
    df = pd.read_csv(GAMES_FILE)
    df["pitcher_home"] = df["pitcher_home"].astype(str).apply(normalize_name)
    df["pitcher_away"] = df["pitcher_away"].astype(str).apply(normalize_name)
    return df

def load_pitchers():
    # --- ADDED DEBUG PRINTS FOR FILE PATH ---
    print(f"DEBUG: Checking PITCHERS_FILE path: {PITCHERS_FILE}")
    if not os.path.exists(PITCHERS_FILE):
        print(f"ERROR: PITCHERS_FILE does not exist at: {PITCHERS_FILE}")
        # Optionally, raise an error or return an empty DataFrame
        return pd.DataFrame(columns=['name'])
    else:
        print(f"DEBUG: PITCHERS_FILE exists.")
    # --- END DEBUG PRINTS FOR FILE PATH ---

    df = pd.read_csv(PITCHERS_FILE)
    
    # --- ADDED DEBUG PRINTS BEFORE NORMALIZATION ---
    print("DEBUG: Raw Pitchers DataFrame (first 10 names from PITCHERS_FILE) BEFORE normalization:")
    print(df["name"].head(10).tolist())
    # Check for McCullers in raw data (case-insensitive)
    mccullers_raw = df[df["name"].astype(str).str.contains("mccullers", case=False, na=False)]
    if not mccullers_raw.empty:
        print("DEBUG: 'Mccullers' (case-insensitive) found in RAW pitchers_df. All found names (repr):")
        for name in mccullers_raw["name"].tolist():
            print(f"  - {repr(name)}") # Use repr to show hidden chars
    else:
        print("DEBUG: No 'Mccullers' (case-insensitive) found in RAW pitchers_df.")
    # --- END DEBUG PRINTS BEFORE NORMALIZATION ---

    df["name"] = df["name"].astype(str).apply(normalize_name)
    
    # --- ADDED DEBUG PRINTS AFTER NORMALIZATION ---
    print("DEBUG: Pitchers DataFrame (first 10 names) AFTER normalization:")
    print(df["name"].head(10).tolist())
    
    mccullers_in_pitchers_df_normalized = df[df["name"].str.contains("mccullers", case=False, na=False)]
    if not mccullers_in_pitchers_df_normalized.empty:
        print("DEBUG: 'Mccullers' (case-insensitive) found in NORMALIZED pitchers_df. All found names (repr):")
        for name in mccullers_in_pitchers_df_normalized["name"].tolist():
            print(f"  - {repr(name)}") # Use repr to show hidden chars
    else:
        print("DEBUG: No 'Mccullers' (case-insensitive) found in NORMALIZED pitchers_df.")
    # --- END ADDED DEBUG PRINTS ---
    
    return df

def filter_and_tag(pitchers_df, games_df, side):
    key = f"pitcher_{side}"
    team_key = f"{side}_team"
    tagged = []
    missing = []

    # Get all normalized pitcher names from the pitchers_df for easy lookup
    normalized_pitcher_names_set = set(pitchers_df["name"])

    for _, row in games_df.iterrows():
        pitcher_name_from_games_df = row[key] # This name is already normalized by load_games()
        team_name = row[team_key] # This line defines team_name

        # --- DEBUG PRINT STATEMENTS START ---
        # Only print for the relevant pitcher to avoid excessive output
        if "mccullers" in pitcher_name_from_games_df.lower():
            print(f"DEBUG: Game Pitcher ({side}): '{pitcher_name_from_games_df}' (Type: {type(pitcher_name_from_games_df)}, Len: {len(pitcher_name_from_games_df)})")
            
            # This line had a typo - corrected from `found_in_pitcher_names_set` to `found_in_pitchers_df`
            found_in_pitchers_df = pitcher_name_from_games_df in normalized_pitcher_names_set
            print(f"DEBUG: Is Game Pitcher found in Pitchers DataFrame? {found_in_pitchers_df}")
            
            if not found_in_pitchers_df:
                print(f"DEBUG: Available Pitchers (first 5 for context, then specific McCullers variations):")
                # Print the first few entries to see the general format of names in the set
                print(list(normalized_pitcher_names_set)[:5]) 
                # Iterate through sorted pitcher names to find and print McCullers variations
                import difflib # Import difflib here for local scope if not already at top
                for p_name in sorted(list(normalized_pitcher_names_set)):
                    if "mccullers" in p_name.lower():
                        print(f"  - Found in pitchers_df: '{p_name}' (Type: {type(p_name)}, Len: {len(p_name)})")
                        # Detailed comparison if they look the same but don't match
                        if pitcher_name_from_games_df == p_name:
                             print("  --- ERROR: They appear identical but aren't matching! Check invisible chars. ---")
                             print(f"  repr(Game): {repr(pitcher_name_from_games_df)}")
                             print(f"  repr(Pitcher): {repr(p_name)}")
                             diff = list(difflib.ndiff(pitcher_name_from_games_df, p_name))
                             if any(d.startswith('+') or d.startswith('-') for d in diff):
                                 print("  Differences found (difflib):")
                                 print("".join(diff))
        # --- DEBUG PRINT STATEMENTS END ---

        matched = pitchers_df[pitchers_df["name"] == pitcher_name_from_games_df].copy()

        if matched.empty:
            missing.append(pitcher_name_from_games_df)
        else:
            matched[team_key] = team_name
            tagged.append(matched)

    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        if side == "home":
            df = df.rename(columns={"home_team": "team"})
        else:
            df = df.rename(columns={"away_team": "team"})
        return df, missing
    return pd.DataFrame(columns=pitchers_df.columns.tolist() + [team_key]), missing

def main():
    games_df = load_games()
    pitchers_df = load_pitchers()

    home_df, home_missing = filter_and_tag(pitchers_df, games_df, "home")
    away_df, away_missing = filter_and_tag(pitchers_df, games_df, "away")

    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)

    print(f"✅ Wrote {len(home_df)} rows to {OUT_HOME}")
    print(f"✅ Wrote {len(away_df)} rows to {OUT_AWAY}")

    if home_missing:
        print("\n=== MISSING HOME PITCHERS ===")
        for name in sorted(set(home_missing)):
            print(name)
    if away_missing:
        print("\n=== MISSING AWAY PITCHERS ===")
        for name in sorted(set(away_missing)):
            print(name)

if __name__ == "__main__":
    main()
