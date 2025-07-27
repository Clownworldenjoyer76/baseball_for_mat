import pandas as pd
from pathlib import Path

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUT_HOME = "data/adjusted/pitchers_home.csv"
OUT_AWAY = "data/adjusted/pitchers_away.csv"

def load_games():
    return pd.read_csv(GAMES_FILE)[["home_team", "away_team"]].drop_duplicates()

def load_pitchers():
    df = pd.read_csv(PITCHERS_FILE)
    df["name"] = df["last_name"].astype(str).str.strip() + ", " + df["first_name"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["name", "team"])
    return df

def filter_and_tag(pitchers_df, games_df, side):
    key = f"pitcher_{side}"
    team_key = f"{side}_team"
    opponent_key = "away_team" if side == "home" else "home_team"
    tagged = []
    missing = []
    unmatched_teams = []

    # Load full matchup data for pitcher name matching
    full_games_df = pd.read_csv(GAMES_FILE)
    full_games_df[key] = full_games_df[key].astype(str).str.strip()
    full_games_df[team_key] = full_games_df[team_key].astype(str).str.strip()

    for _, row in full_games_df.iterrows():
        pitcher_name = row[key]
        team_name = row[team_key]
        opponent_team = row[opponent_key]

        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()

        if matched.empty:
            missing.append(pitcher_name)
            unmatched_teams.append(team_name)
        else:
            matched["team"] = team_name
            matched["home_away"] = side
            matched[opponent_key] = opponent_team
            tagged.append(matched)

    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        df.drop(columns=[col for col in df.columns if col.endswith(".1")], inplace=True)
        df.sort_values(by=["team", "name"], inplace=True)
        df.drop_duplicates(inplace=True)
        return df, missing, unmatched_teams

    return pd.DataFrame(columns=pitchers_df.columns.tolist() + ["team", "home_away", opponent_key]), missing, unmatched_teams

def main():
    games_df = load_games()
    pitchers_df = load_pitchers()

    home_df, home_missing, home_unmatched_teams = filter_and_tag(pitchers_df, games_df, "home")
    away_df, away_missing, away_unmatched_teams = filter_and_tag(pitchers_df, games_df, "away")

    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)

    print(f"✅ Wrote {len(home_df)} rows to {OUT_HOME}")
    print(f"✅ Wrote {len(away_df)} rows to {OUT_AWAY}")

    expected = len(games_df) * 2
    actual = len(home_df) + len(away_df)
    if actual != expected:
        print(f"⚠️ Mismatch: Expected {expected} total pitchers, but got {actual}")

    if home_missing:
        print("\n=== MISSING HOME PITCHERS ===")
        for name in sorted(set(home_missing)):
            print(name)

    if away_missing:
        print("\n=== MISSING AWAY PITCHERS ===")
        for name in sorted(set(away_missing)):
            print(name)

    unmatched = set(home_unmatched_teams + away_unmatched_teams)
    if unmatched:
        print(f"\n⚠️ Unmatched team count: {len(unmatched)}")
        print("Top 5 unmatched teams:")
        for team in sorted(unmatched)[:5]:
            print(f" - {team}")

if __name__ == "__main__":
    main()
