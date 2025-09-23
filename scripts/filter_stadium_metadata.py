import pandas as pd
import os
import subprocess
import logging

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STADIUM_FILE = "data/Data/stadium_metadata.csv"
LOG_DIR = "summaries"
os.makedirs(LOG_DIR, exist_ok=True)

# --- Helper function for running Git commands ---
def run_git_command(command_parts, success_message, error_prefix, log_output=False):
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

# --- Row filter logic abstracted ---
def clean_stadium_dataframe(df):
    df["away_team"] = df["away_team"].fillna("").astype(str).str.strip()
    return df[df["away_team"] != ""]

def filter_stadium_metadata():
    if not os.path.exists(STADIUM_FILE):
        logger.error(f"❌ Stadium metadata file not found: {STADIUM_FILE}. Exiting.")
        return

    df = pd.read_csv(STADIUM_FILE)
    original_rows = len(df)

    df_cleaned = clean_stadium_dataframe(df)
    cleaned_rows = len(df_cleaned)
    removed = original_rows - cleaned_rows

    df_cleaned.to_csv(STADIUM_FILE, index=False)
    logger.info(f"✅ Cleaned stadium metadata written to {STADIUM_FILE}")
    logger.info(f"Summary for filter_stadium_metadata.py:")
    logger.info(f"Original rows: {original_rows}")
    logger.info(f"Removed rows: {removed}")
    logger.info(f"Remaining rows: {cleaned_rows}")

    stadium_commit_made = False

    if run_git_command(["git", "add", STADIUM_FILE], f"Git add successful for {STADIUM_FILE}", f"Git add failed for {STADIUM_FILE}"):
        diff_check = subprocess.run(["git", "diff", "--cached", "--exit-code", STADIUM_FILE], capture_output=True, text=True)
        if diff_check.returncode == 0:
            logger.warning(f"⚠️ No changes detected for {STADIUM_FILE} to commit. Skipping commit.")
            if diff_check.stdout: logger.debug(f"Git diff stdout (no changes): {diff_check.stdout.strip()}")
            if diff_check.stderr: logger.debug(f"Git diff stderr (no changes): {diff_check.stderr.strip()}")
        elif diff_check.returncode == 1:
            commit_message = f"🧹 Removed empty away_team rows from stadium_metadata.csv ({removed} rows removed)"
            if run_git_command(["git", "commit", "-m", commit_message], f"Git commit successful for {STADIUM_FILE}", f"Git commit failed for {STADIUM_FILE}", log_output=True):
                stadium_commit_made = True
                logger.info(f"✅ Committed changes to {STADIUM_FILE}.")
            else:
                logger.error(f"❌ Failed to commit {STADIUM_FILE}. See previous error.")
        else:
            logger.error(f"❌ Unexpected return code from git diff for {STADIUM_FILE}: {diff_check.returncode}.")
            if diff_check.stdout: logger.error(f"Git diff stdout:\n{diff_check.stdout.strip()}")
            if diff_check.stderr: logger.error(f"Git diff stderr:\n{diff_check.stderr.strip()}")
    else:
        logger.error(f"❌ Could not add {STADIUM_FILE} to staging.")

    if stadium_commit_made:
        if run_git_command(["git", "push"], "Git push successful", "Git push failed"):
            logger.info("✅ Committed changes to stadium_metadata.csv pushed.")
        else:
            logger.error("❌ Git push failed from filter_stadium_metadata.py. Check previous errors.")
    else:
        logger.info("ℹ️ No new stadium_metadata.csv commits were made by filter_stadium_metadata.py to push.")

if __name__ == "__main__":
    filter_stadium_metadata()
