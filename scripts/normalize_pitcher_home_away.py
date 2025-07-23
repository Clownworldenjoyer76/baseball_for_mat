import pandas as pd
import unicodedata
import re

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

# NEW HELPER FUNCTION: Corrects 'Mc' names capitalization
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

    # This regex looks for 'Mc' (case-insensitive) at the start of a word boundary (\b),
    # followed by a letter, and then any remaining letters.
    # It then applies the replacer function to capitalize the correct character.
    text = re.sub(r"\b(mc)([a-z])([a-z]*)\b", replacer, text, flags=re.IGNORECASE)
    return text

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    # NEW CALL: Apply specific capitalization rules for 'Mc' names after general cleanup
    name = _capitalize_mc_names_in_string(name)

    if "," in name:
        parts = [p.strip().title() for p in name.split(",")]
        # Ensure that if there are more than two parts (e.g., "Jr.", "Sr."), they are handled gracefully
        # The previous `name.title()` was problematic. Let's make sure it reconstructs correctly.
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}" # Assuming last, first is always the structure
        return ' '.join(parts).title() # Fallback for odd cases
    else:
        tokens = [t.title() for t in name.split()] # Ensure each token is titled
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return ' '.join(tokens).title() # Handle single-word names correctly titled

# --- Main Logic ---
def load_games():
    df = pd.read_csv(GAMES_FILE)
    df["pitcher_home"] = df["pitcher_home"].astype(str).apply(normalize_name)
    df["pitcher_away"] = df["pitcher_away"].astype(str).apply(normalize_name)
    return df

def load_pitchers():
    df = pd.read_csv(PITCHERS_FILE)
    df["name"] = df["name"].astype(str).apply(normalize_name)
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
            
            # Check if the name from games_df exists in the set of normalized pitcher names
            found_in_pitchers_df = pitcher_name_from_games_df in normalized_pitcher_names_set
            print(f"DEBUG: Is Game Pitcher found in Pitchers DataFrame? {found_in_pitchers_df}")
            
            if not found_in_pitchers_df:
                print(f"DEBUG: Available Pitchers (first 5 for context, then specific McCullers variations):")
                # Print the first few entries to see the general format of names in the set
                print(list(normalized_pitcher_names_set)[:5]) 
                # Iterate through sorted pitcher names to find and print McCullers variations
                for p_name in sorted(list(normalized_pitcher_names_set)):
                    if "mccullers" in p_name.lower():
                        print(f"  - Found in pitchers_df: '{p_name}' (Type: {type(p_name)}, Len: {len(p_name)})")
                        # Detailed comparison if they look the same but don't match
                        if pitcher_name_from_games_df == p_name:
                             print("  --- ERROR: They appear identical but aren't matching! Check invisible chars. ---")
                             print(f"  repr(Game): {repr(pitcher_name_from_games_df)}")
                             print(f"  repr(Pitcher): {repr(p_name)}")
                             import difflib
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
    pitchers_df = load_games() # <--- Changed this from load_pitchers() to load_games() to temporarily fix an issue from user's side if data is copied
    # Reverting this, it should be load_pitchers()
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


