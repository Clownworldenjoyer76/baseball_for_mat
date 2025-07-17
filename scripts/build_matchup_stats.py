import pandas as pd
from pathlib import Path
import subprocess

def build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games):
    # Normalize team casing
    batters_home['team'] = batters_home['team'].str.upper()
    batters_away['team'] = batters_away['team'].str.upper()
    pitchers_home['home_team'] = pitchers_home['home_team'].str.upper()
    pitchers_away['team'] = pitchers_away['team'].str.upper()
    games['home_team'] = games['home_team'].str.upper()
    games['away_team'] = games['away_team'].str.upper()

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
    pitchers_away = pd.read_csv("data/adjusted/pitchers_away_park.csv")
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")

    matchup_stats = build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games)
    save_output(matchup_stats)

if __name__ == "__main__":
    main()
