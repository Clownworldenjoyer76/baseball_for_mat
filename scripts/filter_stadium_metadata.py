import pandas as pd
import os
from datetime import datetime
import subprocess
import logging

# --- Logging Setup ---
# Configure logging to output to stdout/stderr so it gets captured by the YAML's &>> redirection.
# Set level to INFO for general messages, DEBUG for more verbose Git command outputs.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STADIUM_FILE = "data/Data/stadium_metadata.csv"
LOG_DIR = "summaries" # Still needed for other summaries, but not for this specific log file anymore
os.makedirs(LOG_DIR, exist_ok=True) # Ensure summaries directory exists

# --- Helper function for running Git commands (re-using the robust version) ---
def run_git_command(command_parts, success_message, error_prefix, log_output=False):
    """
    Runs a git command and logs its output/errors.
    log_output: if True, will log stdout/stderr of the command regardless of success.
    """
    try:
        result = subprocess.run(command_parts, check=True, capture_output=True, text=True, cwd=os.getcwd())
        logger.debug(f"{success_message}: {' '.join(command_parts)}")
        if log_output and result.stdout:
            logger.debug(f"Git stdout: {result.stdout.strip()}")
        if log_output and result.stderr:
            logger.debug(f"Git stderr: {result.stderr.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {error_prefix}: {' '.join(command_parts)} returned non-zero exit status {e.returncode}.")
        if e.stdout:
            logger.error(f"Git stdout:\n{e.stdout.strip()}")
        if e.stderr:
            logger.error(f"Git stderr:\n{e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"❌ Error: 'git' command not found. Is Git installed and in PATH?")
        return False

def filter_stadium_metadata():
    if not os.path.exists(STADIUM_FILE):
        logger.error(f"❌ Stadium metadata file not found: {STADIUM_FILE}. Exiting.")
        return

    df = pd.read_csv(STADIUM_FILE)
    original_rows = len(df)
    
    # Remove rows where away_team is blank or NaN
    df_cleaned = df[df["away_team"].notna() & (df["away_team"].astype(str).str.strip() != "")]
    cleaned_rows = len(df_cleaned)
    removed = original_rows - cleaned_rows

    # Save cleaned file
    df_cleaned.to_csv(STADIUM_FILE, index=False)
    logger.info(f"✅ Cleaned stadium metadata written to {STADIUM_FILE}")

    # Log results using Python's logging, which will go to summaries/log.txt
    logger.info(f"Summary for filter_stadium_metadata.py:")
    logger.info(f"Original rows: {original_rows}")
    logger.info(f"Removed rows: {removed}")
    logger.info(f"Remaining rows: {cleaned_rows}")

    # --- Git commit and push logic ---
    
    # 1. Handle stadium_metadata.csv
    stadium_commit_made = False # Flag to track if this specific commit happened
    
    if run_git_command(["git", "add", STADIUM_FILE], f"Git add successful for {STADIUM_FILE}", f"Git add failed for {STADIUM_FILE}"):
        diff_check = subprocess.run(["git", "diff", "--cached", "--exit-code", STADIUM_FILE], capture_output=True)
        if diff_check.returncode == 0:
            logger.warning(f"⚠️ No changes detected for {STADIUM_FILE} to commit. Skipping commit.")
            if diff_check.stdout: logger.debug(f"Git diff stdout (no changes): {diff_check.stdout.decode().strip()}")
            if diff_check.stderr: logger.debug(f"Git diff stderr (no changes): {diff_check.stderr.decode().strip()}")
        elif diff_check.returncode == 1:
            commit_message = f"🧹 Removed empty away_team rows from stadium_metadata.csv ({removed} rows removed)"
            if run_git_command(["git", "commit", "-m", commit_message], f"Git commit successful for {STADIUM_FILE}", f"Git commit failed for {STADIUM_FILE}", log_output=True):
                stadium_commit_made = True
                logger.info(f"✅ Committed changes to {STADIUM_FILE}.")
            else:
                logger.error(f"❌ Failed to commit {STADIUM_FILE}. See previous error.")
        else:
            logger.error(f"❌ Unexpected return code from git diff for {STADIUM_FILE}: {diff_check.returncode}.")
            if diff_check.stdout: logger.error(f"Git diff stdout:\n{diff_check.stdout.decode().strip()}")
            if diff_check.stderr: logger.error(f"Git diff stderr:\n{diff_check.stderr.decode().strip()}")
    else:
        logger.error(f"❌ Could not add {STADIUM_FILE} to staging.")

    # 2. Removed logic for generating and committing a separate log file.
    #    All relevant log output is now handled by the Python logging module,
    #    which goes to the unified summaries/log.txt via the YAML redirection.

    # 3. Final Push (only if the stadium_metadata.csv commit happened)
    # IMPORTANT: If you want all commits from all scripts to be pushed ONLY by the final YAML step,
    # then COMMENT OUT THIS git push call.
    # Otherwise, it will push immediately after this script if changes were committed.
    if stadium_commit_made:
        if run_git_command(["git", "push"], "Git push successful", "Git push failed"):
            logger.info("✅ Committed changes to stadium_metadata.csv pushed.")
        else:
            logger.error("❌ Git push failed from filter_stadium_metadata.py. Check previous errors.")
    else:
        logger.info("ℹ️ No new stadium_metadata.csv commits were made by filter_stadium_metadata.py to push.")


if __name__ == "__main__":
    filter_stadium_metadata()
