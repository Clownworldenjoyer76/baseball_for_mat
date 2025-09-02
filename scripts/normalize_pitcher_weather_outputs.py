import pandas as pd
from pathlib import Path
import subprocess
import sys

# Inputs/outputs
STADIUM_MASTER = Path("data/manual/stadium_master.csv")
PITCHERS_HOME_IN = Path("data/adjusted/pitchers_home.csv")
PITCHERS_AWAY_IN = Path("data/adjusted/pitchers_away.csv")
PITCHERS_HOME_OUT = Path("data/adjusted/pitchers_home.csv")
PITCHERS_AWAY_OUT = Path("data/adjusted/pitchers_away.csv")

# Required schemas
REQUIRED_STADIUM_COLS = {
    "team_id", "team_name", "venue", "city", "state", "timezone",
    "is_dome", "latitude", "longitude", "home_team"
}
REQUIRED_PITCHER_ID_COLS = {"player_id", "game_id"}

def fail(msg: str) -> None:
    print(f"INSUFFICIENT INFORMATION: {msg}", file=sys.stderr)
    sys.exit(1)

def validate_stadium_master(path: Path) -> None:
    if not path.exists():
        fail(f"Missing file: {path}")
    df = pd.read_csv(path)
    missing = REQUIRED_STADIUM_COLS.difference(df.columns)
    if missing:
        fail(f"{path} missing columns: {sorted(missing)}")

def validate_pitcher_file_has_ids(path: Path) -> pd.DataFrame:
    if not path.exists():
        fail(f"Missing file: {path}")
    df = pd.read_csv(path)
    missing = REQUIRED_PITCHER_ID_COLS.difference(df.columns)
    if missing:
        fail(f"{path} missing required ID columns: {sorted(missing)}")
    return df

def commit_outputs() -> None:
    # Preserve workflow’s existing behavior
    subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
    subprocess.run(["git", "add", str(PITCHERS_HOME_OUT), str(PITCHERS_AWAY_OUT)], check=True)
    subprocess.run(["git", "commit", "--allow-empty", "-m", "Normalize pitchers before weather adjustment"], check=True)
    subprocess.run(["git", "push"], check=True)
    print("Committed normalization stage outputs.")

def main() -> None:
    # 1) Schema check only; no name/team normalization or matching
    validate_stadium_master(STADIUM_MASTER)

    # 2) Ensure we will march by IDs only
    home_df = validate_pitcher_file_has_ids(PITCHERS_HOME_IN)
    away_df = validate_pitcher_file_has_ids(PITCHERS_AWAY_IN)

    # 3) Write back unchanged (pass-through)
    home_df.to_csv(PITCHERS_HOME_OUT, index=False)
    away_df.to_csv(PITCHERS_AWAY_OUT, index=False)

    # 4) Commit (kept to match workflow expectations)
    commit_outputs()

if __name__ == "__main__":
    main()
