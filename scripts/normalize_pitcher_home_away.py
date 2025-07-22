import pandas as pd

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUT_HOME = "data/adjusted/pitchers_home.csv"
OUT_AWAY = "data/adjusted/pitchers_away.csv"

def load_games():
    games_df = pd.read_csv(GAMES_FILE)
    games_df["pitcher_home"] = games_df["pitcher_home"].astype(str).str.strip()
    games_df["pitcher_away"] = games_df["pitcher_away"].astype(str).str.strip()
    return games_df

def load_pitchers():
    df = pd.read_csv(PITCHERS_FILE)
    df["name"] = df["name"].astype(str).str.strip()
    return df

def filter_and_tag(pitchers_df, games_df, side):
    key = f"pitcher_{side}"
    team_key = f"{side}_team"
    tagged = []
    for _, row in games_df.iterrows():
        pitcher_name = row[key]
        team_name = row[team_key]
        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()
        if not matched.empty:
            matched[team_key] = team_name
            tagged.append(matched)
    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        if side == "home":
            df = df.rename(columns={"home_team": "team"})  # ✅ FIX HERE
        return df
    return pd.DataFrame(columns=pitchers_df.columns.tolist() + [team_key])

def main():
    games_df = load_games()
    pitchers_df = load_pitchers()
    home_df = filter_and_tag(pitchers_df, games_df, "home")
    away_df = filter_and_tag(pitchers_df, games_df, "away")
    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)
    print(f"✅ Wrote {len(home_df)} rows to {OUT_HOME}")
    print(f"✅ Wrote {len(away_df)} rows to {OUT_AWAY}")

if __name__ == "__main__":
    main()
