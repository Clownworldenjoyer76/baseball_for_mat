import pandas as pd
import os
from datetime import datetime
import subprocess
import logging

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STADIUM_FILE = "data/Data/stadium_metadata.csv"
LOG_DIR = "summaries"
os.makedirs(LOG_DIR, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file_path = os.path.join(LOG_DIR, f"log_filter_stadium_metadata_{timestamp}.txt")

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

    # Log results to the specific log file for this script
    with open(log_file_path, "w") as f:
        f.write(f"✅ filter_stadium_metadata.py executed at {timestamp}\n")
        f.write(f"Original rows: {original_rows}\n")
        f.write(f"Removed rows: {removed}\n")
        f.write(f"Remaining rows: {cleaned_rows}\n")
    logger.info(f"📝 Run log written to {log_file_path}")

    # --- Git commit and push logic ---
    
    # 1. Handle stadium_metadata.csv
    stadium_commit_needed = False
    if run_git_command(["git", "add", STADIUM_FILE], f"Git add successful for {STADIUM_FILE}", f"Git add failed for {STADIUM_FILE}"):
        diff_check = subprocess.run(["git", "diff", "--cached", "--exit-code", STADIUM_FILE], capture_output=True)
        if diff_check.returncode == 0:
            logger.warning(f"⚠️ No changes detected for {STADIUM_FILE} to commit. Skipping commit.")
        elif diff_check.returncode == 1:
            stadium_commit_needed = True
            commit_message = f"🧹 Removed empty away_team rows from stadium_metadata.csv ({removed} rows removed)"
            if not run_git_command(["git", "commit", "-m", commit_message], f"Git commit successful for {STADIUM_FILE}", f"Git commit failed for {STADIUM_FILE}", log_output=True):
                logger.error(f"❌ Failed to commit {STADIUM_FILE}. See previous error.")
        else:
            logger.error(f"❌ Unexpected return code from git diff for {STADIUM_FILE}: {diff_check.returncode}.")
    else:
        logger.error(f"❌ Could not add {STADIUM_FILE} to staging.")

    # 2. Handle the specific log file created by this script
    log_file_commit_needed = False
    if run_git_command(["git", "add", log_file_path], f"Git add successful for {log_file_path}", f"Git add failed for {log_file_path}"):
        diff_check_log = subprocess.run(["git", "diff", "--cached", "--exit-code", log_file_path], capture_output=True)
        if diff_check_log.returncode == 0:
            logger.warning(f"⚠️ No changes detected for {log_file_path} to commit. Skipping commit.")
        elif diff_check_log.returncode == 1:
            log_file_commit_needed = True
            commit_message = f"📝 Log: filter_stadium_metadata.py at {timestamp}"
            if not run_git_command(["git", "commit", "-m", commit_message], f"Git commit successful for {log_file_path}", f"Git commit failed for {log_file_path}", log_output=True):
                logger.error(f"❌ Failed to commit {log_file_path}. See previous error.")
        else:
            logger.error(f"❌ Unexpected return code from git diff for {log_file_path}: {diff_check_log.returncode}.")
    else:
        logger.error(f"❌ Could not add {log_file_path} to staging.")

    # 3. Final Push (only if at least one of the commits happened)
    # IMPORTANT: If you want all commits from all scripts to be pushed ONLY by the final YAML step,
    # then COMMENT OUT THIS git push call.
    # Otherwise, it will push immediately after this script.
    if stadium_commit_needed or log_file_commit_needed:
        if run_git_command(["git", "push"], "Git push successful", "Git push failed"):
            logger.info("✅ All changes committed and pushed from filter_stadium_metadata.py.")
        else:
            logger.error("❌ Final Git push failed from filter_stadium_metadata.py. Check previous errors.")
    else:
        logger.info("ℹ️ No new commits were made by filter_stadium_metadata.py to push.")


if __name__ == "__main__":
    filter_stadium_metadata()
