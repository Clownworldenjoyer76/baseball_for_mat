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

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    if "," in name:
        parts = [p.strip().title() for p in name.split(",")]
        return f"{parts[0]}, {parts[1]}" if len(parts) == 2 else name.title()
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            first = tokens[0].title()
            last = " ".join(tokens[1:]).title()
            return f"{last}, {first}"
        return name.title()

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
