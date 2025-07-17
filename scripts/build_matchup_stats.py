import pandas as pd
from pathlib import Path
import subprocess

TEAM_MAP_FILE = "data/Data/team_name_master.csv"

def load_team_mapping():
    df = pd.read_csv(TEAM_MAP_FILE)
    return dict(zip(df['team_name'].str.strip(), df['team_code'].str.strip()))

def normalize_teams(df, column):
    team_map = load_team_mapping()
    df[column] = df[column].map(lambda x: team_map.get(str(x).strip(), str(x).strip()))
    return df

def build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games):
    batters_home = normalize_teams(batters_home, 'team')
    batters_away = normalize_teams(batters_away, 'team')
    pitchers_home = normalize_teams(pitchers_home, 'home_team')
    pitchers_away = normalize_teams(pitchers_away, 'team')
    games = normalize_teams(games, 'home_team')
    games = normalize_teams(games, 'away_team')

    pitchers_home = pitchers_home.drop_duplicates(subset=['home_team'], keep='first')
    pitchers_away = pitchers_away.drop_duplicates(subset=['team'], keep='first')

    matchups = []
    for _, row in games.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']

        home_batters = batters_home[batters_home['team'] == home_team]
        away_batters = batters_away[batters_away['team'] == away_team]

        home_pitcher = pitchers_home[pitchers_home['home_team'] == home_team]
        away_pitcher = pitchers_away[pitchers_away['team'] == away_team]

        matchup = {
            'home_team': home_team,
            'away_team': away_team,
            'home_batters_count': len(home_batters),
            'away_batters_count': len(away_batters),
            'home_pitcher_name': home_pitcher['pitcher'].values[0] if not home_pitcher.empty else 'Unknown',
            'away_pitcher_name': away_pitcher['pitcher'].values[0] if not away_pitcher.empty else 'Unknown',
        }
        matchups.append(matchup)

    return pd.DataFrame(matchups)

def save_output(df):
    output_path = Path("data/final")
    output_path.mkdir(parents=True, exist_ok=True)
    outfile = output_path / "matchup_stats.csv"
    df.to_csv(outfile, index=False)

    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", str(outfile)], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: matchup stats built"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Matchup stats committed.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    batters_home = pd.read_csv("data/adjusted/batters_home_weather_park.csv")
    batters_away = pd.read_csv("data/adjusted/batters_away_weather_park.csv")
    pitchers_home = pd.read_csv("data/adjusted/pitchers_home_weather_park.csv")
    pitchers_away = pd.read_csv("data/adjusted/pitchers_away_weather_park.csv")
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")

    matchup_stats = build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games)
    save_output(matchup_stats)

if __name__ == "__main__":
    main()
