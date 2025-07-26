# scripts/commit_split_batters.py

import subprocess
from datetime import datetime
import os
import logging
from zoneinfo import ZoneInfo

# --- Configuration ---
AUTO_PUSH = True  # Set to False to skip pushing after commit
FILES_TO_COMMIT = [
    "data/adjusted/batters_home.csv",
    "data/adjusted/batters_away.csv"
]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper: Clean Git Output ---
def clean_output(text_bytes):
    return text_bytes.decode().strip() if text_bytes else ""

# --- Helper: Run Git Commands ---
def run_git_command(command_parts, success_message, error_prefix, log_output=False):
    try:
        result = subprocess.run(command_parts, check=True, capture_output=True, text=True)
        logger.info(success_message)
        if log_output:
            if result.stdout:
                logger.debug(f"Git stdout:\n{clean_output(result.stdout.encode())}")
            if result.stderr:
                logger.debug(f"Git stderr:\n{clean_output(result.stderr.encode())}")
        return True
    except FileNotFoundError:
        logger.critical("❌ Error: 'git' command not found. Ensure Git is installed and available in PATH.")
        exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"{error_prefix}: {' '.join(command_parts)}")
        if e.stdout:
            logger.error(f"Git stdout:\n{clean_output(e.stdout.encode())}")
        if e.stderr:
            logger.error(f"Git stderr:\n{clean_output(e.stderr.encode())}")
        return False

# --- Helper: Check for Staged Changes ---
def has_staged_changes():
    result = subprocess.run(["git", "diff", "--cached", "--exit-code"], capture_output=True)
    return result.returncode == 1  # 1 means changes staged

# --- Main ---
def main():
    # Use timezone-aware timestamp for accurate logging
    timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %I:%M:%S %p %Z")

    # Check for missing files
    missing = [f for f in FILES_TO_COMMIT if not os.path.exists(f)]
    if missing:
        for f in missing:
            logger.error(f"❌ Missing required file: {f}")
        logger.critical("🚫 Aborting Git commit/push due to missing input files.")
        exit(1)

    # Add files to Git staging
    logger.info(f"🔁 Staging files: {', '.join(FILES_TO_COMMIT)}")
    if not run_git_command(["git", "add"] + FILES_TO_COMMIT,
                           "✅ Git add successful",
                           "❌ Git add failed"):
        return

    if not has_staged_changes():
        logger.warning("⚠️ No changes detected. Nothing to commit.")
        return

    # Commit changes
    commit_msg = f"🔄 Split batters home/away @ {timestamp}"
    logger.info(f"📝 Committing changes with message: {commit_msg}")
    if not run_git_command(["git", "commit", "-m", commit_msg],
                           "✅ Git commit successful",
                           "❌ Git commit failed",
                           log_output=True):
        return

    # Push changes if enabled
    if AUTO_PUSH:
        logger.info("🚀 Pushing changes to remote repository...")
        if run_git_command(["git", "push"],
                           "✅ Git push successful",
                           "❌ Git push failed"):
            logger.info("🎉 Git commit and push complete.")
        else:
            logger.error("❌ Git push failed. Manual intervention may be required.")

if __name__ == "__main__":
    main()
