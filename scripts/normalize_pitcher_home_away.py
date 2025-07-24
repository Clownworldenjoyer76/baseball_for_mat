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
    name = _capitalize_mc_names_in_string(name)

    if "," in name:
        parts = [p.strip().title() for p in name.split(",")]
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}"
        return ' '.join(parts).title()
    else:
        tokens = [t.title() for t in name.split()]
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return ' '.join(tokens).title()

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

    normalized_pitcher_names_set = set(pitchers_df["name"])

    for _, row in games_df.iterrows():
        pitcher_name = row[key]
        team_name = row[team_key]

        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()

        if matched.empty:
            missing.append(pitcher_name)
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
