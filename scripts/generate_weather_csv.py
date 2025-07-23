import pandas as pd
from pathlib import Path
import subprocess
import os # Import os for path checking

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def generate_weather_csv():
    print("DEBUG: Starting generate_weather_csv.py script.")

    # --- Debug: Check if input files exist ---
    for f_path in [GAMES_FILE, STADIUM_FILE, TEAM_MAP_FILE]:
        if not os.path.exists(f_path):
            print(f"ERROR: Required input file not found: {f_path}")
            # Optionally, you might want to raise an exception or exit here
            return

    try:
        games_df = pd.read_csv(GAMES_FILE)
        stadium_df = pd.read_csv(STADIUM_FILE)
        team_map_df = pd.read_csv(TEAM_MAP_FILE)
        print(f"DEBUG: Successfully loaded {len(games_df)} games, {len(stadium_df)} stadiums, and {len(team_map_df)} team mappings.")
    except FileNotFoundError as e:
        print(f"❌ File not found during loading: {e}")
        return
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return

    # Debug print for initial dataframe states
    print(f"DEBUG: games_df columns: {games_df.columns.tolist()}")
    print(f"DEBUG: stadium_df columns: {stadium_df.columns.tolist()}")
    print(f"DEBUG: team_map_df columns: {team_map_df.columns.tolist()}")

    games_df['home_team'] = games_df['home_team'].str.strip().str.upper()
    games_df['away_team'] = games_df['away_team'].str.strip().str.upper()
    stadium_df['home_team'] = stadium_df['home_team'].str.strip().str.upper()
    team_map_df['uppercase'] = team_map_df['team_name'].str.strip().str.upper()
    team_map_df = team_map_df.drop_duplicates(subset='uppercase')

    games_df = games_df.drop(columns=['game_time'], errors='ignore')

    # --- Debug: First Merge ---
    print(f"DEBUG: Merging games_df ({len(games_df)} rows) with stadium_df ({len(stadium_df)} rows) on 'home_team'.")
    merged = pd.merge(games_df, stadium_df, on='home_team', how='left')
    print(f"DEBUG: After first merge (games + stadium), merged rows: {len(merged)}")
    if merged.empty:
        print("❌ Merge failed: No matching rows after games + stadium merge. Check 'home_team' column data in both DFs.")
        print(f"DEBUG: games_df home_team unique: {games_df['home_team'].unique().tolist()}")
        print(f"DEBUG: stadium_df home_team unique: {stadium_df['home_team'].unique().tolist()}")
        return

    # --- Debug: Second Merge (home_team mapping) ---
    print(f"DEBUG: Merging with team_map_df for home_team. Merged rows before: {len(merged)}")
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']],
        left_on='home_team',
        right_on='uppercase',
        how='left'
    )
    print(f"DEBUG: After second merge (home_team map), merged rows: {len(merged)}")
    merged.drop(columns=['home_team', 'uppercase'], inplace=True)
    merged.rename(columns={'team_name': 'home_team'}, inplace=True)
    print(f"DEBUG: Columns after home_team rename: {merged.columns.tolist()}")


    # --- Debug: away_team column handling ---
    print(f"DEBUG: Checking for away_team column handling.")
    if 'away_team_x' in merged.columns and 'away_team_y' in merged.columns:
        merged['away_team'] = merged['away_team_x'].combine_first(merged['away_team_y'])
        merged.drop(columns=['away_team_x', 'away_team_y'], inplace=True)
        print("DEBUG: Handled away_team_x/away_team_y columns.")
    elif 'away_team' not in merged.columns:
        print("❌ 'away_team' column not found in merged dataframe after initial merges. Check preceding merges.")
        print(f"DEBUG: Current merged columns: {merged.columns.tolist()}")
        return
    else:
        print("DEBUG: 'away_team' column already in expected format.")

    # --- Debug: Third Merge (away_team mapping) ---
    merged['away_team'] = merged['away_team'].str.strip().str.upper()
    print(f"DEBUG: Merging with team_map_df for away_team. Merged rows before: {len(merged)}")
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']],
        left_on='away_team',
        right_on='uppercase',
        how='left'
    )
    print(f"DEBUG: After third merge (away_team map), merged rows: {len(merged)}")
    merged.drop(columns=['away_team', 'uppercase'], inplace=True)
    merged.rename(columns={'team_name': 'away_team'}, inplace=True)
    print(f"DEBUG: Columns after away_team rename: {merged.columns.tolist()}")


    # Ensure output directory exists
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"DEBUG: Wrote {len(merged)} rows to {OUTPUT_FILE}")


    summary = (
        f"✅ Weather input file generated\n"
        f"🔢 Rows: {len(merged)}\n"
        f"📁 Output: {OUTPUT_FILE}\n"
        f"📄 Games file: {GAMES_FILE}\n"
        f"🏟️ Stadium file: {STADIUM_FILE}"
    )
    print(summary)
    Path(SUMMARY_FILE).write_text(summary)
    print(f"DEBUG: Wrote summary to {SUMMARY_FILE}")

    # --- Git Operations ---
    try:
        # Add the generated files
        print(f"DEBUG: Attempting to git add {OUTPUT_FILE} and {SUMMARY_FILE}")
        subprocess.run(["git", "add", OUTPUT_FILE, SUMMARY_FILE], check=True, capture_output=True, text=True)
        print("DEBUG: Git add successful.")

        # Check for changes before committing
        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if not status_output.strip():
            print("DEBUG: No changes to commit after adding weather files.")
        else:
            print("DEBUG: Changes detected, attempting to commit.")
            subprocess.run(["git", "commit", "-m", "📝 Generate weather_input.csv and summary"], check=True, capture_output=True, text=True)
            print("DEBUG: Git commit successful.")
            print("DEBUG: Attempting to git push.")
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True) # Added capture_output/text for more verbose logging if needed
            print("✅ Git commit and push complete.")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed:")
        print(f"  Command: {e.cmd}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT: {e.stdout}")
        print(f"  STDERR: {e.stderr}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations: {e}")

if __name__ == "__main__":
    generate_weather_csv()
