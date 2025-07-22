import pandas as pd

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUT_HOME = "data/adjusted/pitchers_home.csv"
OUT_AWAY = "data/adjusted/pitchers_away.csv"
LOG_FILE = "summaries/log_normalize_pitchers.txt"

def load_games():
    df = pd.read_csv(GAMES_FILE)
    df["pitcher_home"] = df["pitcher_home"].astype(str).str.strip()
    df["pitcher_away"] = df["pitcher_away"].astype(str).str.strip()
    return df

def load_pitchers():
    df = pd.read_csv(PITCHERS_FILE)
    df["name"] = df["name"].astype(str).str.strip()
    return df

def filter_and_tag(pitchers_df, games_df, side):
    key = f"pitcher_{side}"
    team_key = f"{side}_team"
    tagged = []
    missed = []

    for _, row in games_df.iterrows():
        pitcher_name = row[key]
        team_name = row[team_key]
        matched = pitchers_df[pitchers_df["name"].str.lower() == pitcher_name.lower()].copy()
        if not matched.empty:
            matched["team"] = team_name  # Normalize output column name
            tagged.append(matched)
        else:
            missed.append(pitcher_name)

    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        return df, missed
    return pd.DataFrame(columns=pitchers_df.columns.tolist() + ["team"]), missed

def write_log(missed_home, missed_away):
    with open(LOG_FILE, "w") as f:
        f.write("=== MISSING HOME PITCHERS ===\n")
        for name in missed_home:
            f.write(f"{name}\n")
        f.write("\n=== MISSING AWAY PITCHERS ===\n")
        for name in missed_away:
            f.write(f"{name}\n")
    print(f"📄 Log written to {LOG_FILE}")

def main():
    games_df = load_games()
    pitchers_df = load_pitchers()

    home_df, missed_home = filter_and_tag(pitchers_df, games_df, "home")
    away_df, missed_away = filter_and_tag(pitchers_df, games_df, "away")

    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)

    print(f"✅ Wrote {len(home_df)} rows to {OUT_HOME}")
    print(f"✅ Wrote {len(away_df)} rows to {OUT_AWAY}")

    write_log(missed_home, missed_away)

if __name__ == "__main__":
    main()
