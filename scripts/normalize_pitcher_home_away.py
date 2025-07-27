import pandas as pd
import logging
from pathlib import Path

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- File Paths ---
GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
PITCHERS_FILE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
OUT_HOME = Path("data/adjusted/pitchers_home.csv")
OUT_AWAY = Path("data/adjusted/pitchers_away.csv")
TEAM_MAP_FILE = Path("data/Data/team_name_master.csv") # Added this file path

# --- Helper Functions for Team Mapping ---
def load_team_map():
    """Loads the team name mapping from a CSV file."""
    if not TEAM_MAP_FILE.exists():
        logger.critical(f"❌ Missing team mapping file: {TEAM_MAP_FILE}")
        raise FileNotFoundError(f"{TEAM_MAP_FILE} does not exist.")

    df = pd.read_csv(TEAM_MAP_FILE)
    required_cols = {"team_code", "team_name"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"❌ team_name_master.csv must contain columns: {required_cols}")

    df["team_code"] = df["team_code"].astype(str).str.strip()
    df["team_name"] = df["team_name"].astype(str).str.strip()
    return dict(zip(df["team_code"], df["team_name"]))

# --- Original Helper Functions (modified for integration) ---
def load_pitchers():
    """Loads and preprocesses the pitchers data."""
    df = pd.read_csv(PITCHERS_FILE)
    df["name"] = df["last_name"].astype(str).str.strip() + ", " + df["first_name"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["name", "team"])
    return df

def filter_and_tag(pitchers_df: pd.DataFrame, side: str, team_map: dict):
    """
    Filters and tags pitchers as home or away, applying team name standardization.

    Args:
        pitchers_df (pd.DataFrame): DataFrame containing pitcher data.
        side (str): 'home' or 'away' to specify which pitcher/team to filter for.
        team_map (dict): A dictionary for mapping team codes to full names.

    Returns:
        tuple: A tuple containing the filtered DataFrame, a list of missing pitchers,
               and a list of unmatched teams.
    """
    key = f"pitcher_{side}"
    team_key = f"{side}_team"
    opponent_key = "away_team" if side == "home" else "home_team"
    tagged = []
    missing = []
    unmatched_teams = []

    # Load only necessary columns from GAMES_FILE and standardize team names immediately
    full_games_df = pd.read_csv(GAMES_FILE)[["pitcher_home", "pitcher_away", "home_team", "away_team"]]
    full_games_df[key] = full_games_df[key].astype(str).str.strip() # Clean pitcher names

    # Apply standardization to game team names *before* matching
    full_games_df['home_team'] = full_games_df['home_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['home_team'])
    full_games_df['away_team'] = full_games_df['away_team'].astype(str).str.strip().map(team_map).fillna(full_games_df['away_team'])
    full_games_df[team_key] = full_games_df[team_key].astype(str).str.strip() # Ensure 'team_key' column is also clean after map

    for idx, row in full_games_df.iterrows(): # Use idx for potential logging
        pitcher_name = row[key]
        team_name = row[team_key] # This team_name is now standardized
        opponent_team = row[opponent_key] # This opponent_team is also standardized

        # Filter the pitchers_df for the current pitcher name
        matched = pitchers_df[pitchers_df["name"] == pitcher_name].copy()

        if matched.empty:
            missing.append(pitcher_name)
            logger.debug(f"🔍 Pitcher '{pitcher_name}' (Team: {team_name}, Game Index: {idx}) not found in pitchers_normalized_cleaned.csv.")
        else:
            # Assign the *standardized* team name from games_df
            matched["team"] = team_name
            matched["home_away"] = side
            matched[opponent_key] = opponent_team
            tagged.append(matched)
            logger.debug(f"✅ Matched pitcher '{pitcher_name}' for {side} team '{team_name}'.")


    if tagged:
        df = pd.concat(tagged, ignore_index=True)
        # Drop any duplicate columns that might arise from the initial load or merge if not careful
        # (e.g., if pitchers_df itself had 'home_team' or 'away_team' columns initially)
        # This part handles columns ending with .1, which often appear from merges
        df.drop(columns=[col for col in df.columns if col.endswith(".1")], errors='ignore', inplace=True)
        df.sort_values(by=["team", "name"], inplace=True)
        df.drop_duplicates(inplace=True) # Ensure unique pitcher-team entries in the output

        # Final standardization pass on the 'team' column if pitchers_df wasn't perfectly clean
        df["team"] = df["team"].astype(str).str.strip().map(team_map).fillna(df["team"])

        logger.info(f"Generated {len(df)} {side} pitcher records.")
        return df, missing, unmatched_teams

    logger.warning(f"⚠️ No {side} pitchers found or matched.")
    # Return an empty DataFrame with expected columns if no matches
    return pd.DataFrame(columns=pitchers_df.columns.tolist() + ["team", "home_away", opponent_key]), missing, unmatched_teams

# --- Main Execution ---
def main():
    logger.info("Starting pitcher data processing and standardization.")
    team_map = load_team_map() # Load the map once
    logger.info(f"Loaded team map with {len(team_map)} entries.")

    pitchers_df = load_pitchers()
    logger.info(f"Loaded {len(pitchers_df)} unique pitchers from {PITCHERS_FILE}.")

    home_df, home_missing, home_unmatched_teams = filter_and_tag(pitchers_df, "home", team_map)
    away_df, away_missing, away_unmatched_teams = filter_and_tag(pitchers_df, "away", team_map)

    home_df.to_csv(OUT_HOME, index=False)
    away_df.to_csv(OUT_AWAY, index=False)

    logger.info(f"✅ Wrote {len(home_df)} rows to {OUT_HOME}")
    logger.info(f"✅ Wrote {len(away_df)} rows to {OUT_AWAY}")

    # Check for expected vs actual pitchers based on games file
    raw_games_df = pd.read_csv(GAMES_FILE) # Re-read to get total count
    expected = len(raw_games_df) * 2 # Each game has 2 pitchers
    actual = len(home_df) + len(away_df)
    if actual != expected:
        logger.warning(f"⚠️ Mismatch: Expected {expected} total pitchers from {GAMES_FILE}, but got {actual}")
    else:
        logger.info(f"Total pitchers generated ({actual}) matches expected count ({expected}).")


    if home_missing:
        logger.warning(f"\n=== MISSING HOME PITCHERS ({len(set(home_missing))} unique) ===")
        for name in sorted(set(home_missing)):
            logger.warning(f"  - {name}")

    if away_missing:
        logger.warning(f"\n=== MISSING AWAY PITCHERS ({len(set(away_missing))} unique) ===")
        for name in sorted(set(away_missing)):
            logger.warning(f"  - {name}")

    unmatched = set(home_unmatched_teams + away_unmatched_teams)
    if unmatched:
        logger.warning(f"\n⚠️ Unmatched team codes from GAMES_FILE (after initial map attempt): {len(unmatched)}")
        logger.warning("Top 5 unmatched teams:")
        for team in sorted(unmatched)[:5]:
            logger.warning(f"  - {team}")

    logger.info("Pitcher data processing and standardization completed.")

if __name__ == "__main__":
    main()
