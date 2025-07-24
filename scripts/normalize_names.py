import pandas as pd
import unicodedata
import re
import os
import difflib

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
    def replacer(match):
        prefix = match.group(1)
        char_to_capitalize = match.group(2).upper()
        rest_of_name = match.group(3).lower()
        return prefix.capitalize() + char_to_capitalize + rest_of_name
    return re.sub(r"\b(mc)([a-z])([a-z]*)\b", replacer, text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    temp_name = name.title()
    final_normalized_name = _capitalize_mc_names_in_string(temp_name)
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
    print(f"DEBUG: Checking PITCHERS_FILE path: {PITCHERS_FILE}")
    if not os.path.exists(PITCHERS_FILE):
        print(f"ERROR: PITCHERS_FILE does not exist at: {PITCHERS_FILE}")
        return pd.DataFrame(columns=['name'])
    else:
        print(f"DEBUG: PITCHERS_FILE exists.")
    df = pd.read_csv(PITCHERS_FILE)
    print("DEBUG: Raw Pitchers DataFrame (first 10 names from PITCHERS_FILE) BEFORE normalization:")
    print(df["name"].head(10).tolist())
    df["name"] = df["name"].astype(str).apply(normalize_name)
    print("DEBUG: Pitchers DataFrame (first 10 names) AFTER normalization:")
    print(df["name"].head(10).tolist())
    return df


def filter_and_tag(pitchers_df, games_df, side):
    key = f"pitcher_{side}"
    team_key = f"{side}_team"
    tagged = []
    missing = []

    normalized_pitcher_names_set = set(pitchers_df["name"])
    pitcher_list_sorted = sorted(normalized_pitcher_names_set)

    for _, row in games_df.iterrows():
        game_pitcher = row[key]
        team_name = row[team_key]

        if game_pitcher not in normalized_pitcher_names_set:
            print(f"⚠️ MISMATCH: '{game_pitcher}' from games_df not found in normalized pitcher list.")
            close_matches = difflib.get_close_matches(game_pitcher, pitcher_list_sorted, n=3, cutoff=0.6)
            if close_matches:
                print(f"  🔍 Close match candidates:")
                for match in close_matches:
                    print(f"    - {match}")
                print(f"  🔬 Character diff vs closest match ({close_matches[0]}):")
                diff = list(difflib.ndiff(game_pitcher, close_matches[0]))
                print("    " + "".join(diff))
            else:
                print("  ❌ No close matches found.")

        matched = pitchers_df[pitchers_df["name"] == game_pitcher].copy()
        if matched.empty:
            missing.append(game_pitcher)
        else:
            matched[team_key] = team_name
            tagged.append(matched)

    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        df = df.rename(columns={team_key: "team"})
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
