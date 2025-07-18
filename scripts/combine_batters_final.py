import pandas as pd
import subprocess

# Input paths
HOME_INPUT = "data/processed/batters_home_with_pitcher.csv"
AWAY_INPUT = "data/processed/batters_away_with_pitcher.csv"
OUTPUT_FILE = "data/final/matchup_stats.csv"

def main():
    bh = pd.read_csv(HOME_INPUT)
    ba = pd.read_csv(AWAY_INPUT)

    bh["side"] = "home"
    ba["side"] = "away"

    matchup_stats = pd.concat([bh, ba], ignore_index=True)
    matchup_stats.to_csv(OUTPUT_FILE, index=False)

    # Git commit + push
    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Add final combined matchup stats"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
