import pandas as pd
from unidecode import unidecode
import os

# Paths
TODAY_GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHER_REF_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_HOME = "data/adjusted/pitchers_home.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away.csv"
LOG_DIR = "summaries/normalize_pitcher_home_away"
os.makedirs(LOG_DIR, exist_ok=True)

def normalize_name(name):
    if pd.isna(name):
        return ""
    name = unidecode(name).strip()
    name = name.replace(" ,", ",").replace(", ", ",").replace(",", ", ")
    name = " ".join(name.split())
    if "," in name:
        return name.title()
    tokens = name.split()
    if len(tokens) >= 2:
        first = tokens[0]
        last = " ".join(tokens[1:])
        return f"{last}, {first}".title()
    return name.title()

def main():
    if not os.path.exists(TODAY_GAMES_FILE):
        print(f"❌ Missing file: {TODAY_GAMES_FILE}")
        return

    if not os.path.exists(PITCHER_REF_FILE):
        print(f"❌ Missing file: {PITCHER_REF_FILE}")
        return

    games = pd.read_csv(TODAY_GAMES_FILE)
    ref = pd.read_csv(PITCHER_REF_FILE)

    # Normalize ref names
    ref["last_name, first_name"] = ref["last_name, first_name"].astype(str).str.strip().apply(normalize_name)
    ref_names = set(ref["last_name, first_name"])

    # Normalize and clean game pitcher names
    games["pitcher_home"] = games["pitcher_home"].astype(str).str.strip().apply(normalize_name)
    games["pitcher_away"] = games["pitcher_away"].astype(str).str.strip().apply(normalize_name)
    games.dropna(subset=["pitcher_home", "pitcher_away"], inplace=True)

    # Detect unmatched pitchers
    dropped_home = games[~games["pitcher_home"].isin(ref_names)]["pitcher_home"].unique().tolist()
    dropped_away = games[~games["pitcher_away"].isin(ref_names)]["pitcher_away"].unique().tolist()

    log_txt = []
    if dropped_home:
        msg = f"⚠️ Dropped unmatched home pitchers: {dropped_home}"
        print(msg)
        log_txt.append(msg)
    if dropped_away:
        msg = f"⚠️ Dropped unmatched away pitchers: {dropped_away}"
        print(msg)
        log_txt.append(msg)

    # Filter valid rows
    home_df = games[games["pitcher_home"].isin(ref_names)].copy()
    away_df = games[games["pitcher_away"].isin(ref_names)].copy()

    if home_df.empty or away_df.empty:
        print("❌ One or both pitcher outputs would be empty. Aborting.")
        log_txt.append("❌ Aborted: one or both output files would be empty.")
        with open(f"{LOG_DIR}/normalize_pitcher_home_away.txt", "w") as f:
            f.write("\n".join(log_txt))
        return

    # Format
    home_df = home_df.rename(columns={"pitcher_home": "last_name, first_name", "home_team": "team"})
    away_df = away_df.rename(columns={"pitcher_away": "last_name, first_name", "away_team": "team"})
    home_df["type"] = "pitcher"
    away_df["type"] = "pitcher"
    home_df = home_df[["last_name, first_name", "team", "type"]].drop_duplicates()
    away_df = away_df[["last_name, first_name", "team", "type"]].drop_duplicates()

    # Output
    os.makedirs("data/adjusted", exist_ok=True)
    home_df.to_csv(OUTPUT_HOME, index=False)
    away_df.to_csv(OUTPUT_AWAY, index=False)

    msg_home = f"✅ Wrote {len(home_df)} rows to {OUTPUT_HOME}"
    msg_away = f"✅ Wrote {len(away_df)} rows to {OUTPUT_AWAY}"
    print(msg_home)
    print(msg_away)
    log_txt.extend([msg_home, msg_away])

    # Write summary
    with open(f"{LOG_DIR}/normalize_pitcher_home_away.txt", "w") as f:
        f.write("\n".join(log_txt))

if __name__ == "__main__":
    main()
