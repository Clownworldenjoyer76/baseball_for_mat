import subprocess
from datetime import datetime
import os # Import os for os.getcwd()
import logging # Import logging

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, # Set to INFO for general messages
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper function for running Git commands ---
def run_git_command(command_parts, success_message, error_prefix, log_output=False):
    """
    Runs a git command and logs its output/errors.
    log_output: if True, will log stdout/stderr of the command regardless of success.
    """
    try:
        # cwd=os.getcwd() ensures the command runs in the expected directory
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

# --- Main Script Logic ---
def main():
    # Use timezone-aware timestamp for consistency if needed, otherwise stick to simple strftime
    timestamp = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p %Z")

    files_to_commit = [
        "data/adjusted/batters_home.csv",
        "data/adjusted/batters_away.csv"
    ]

    # Ensure files exist before attempting to add them
    for f in files_to_commit:
        if not os.path.exists(f):
            logger.error(f"❌ File not found: {f}. Cannot commit.")
            # Depending on your workflow, you might want to exit here or log a critical error.
            # For now, we'll let it continue to see if other files can be added/committed.
            return # Exit if a crucial file is missing

    # 1. Add all relevant files to the staging area
    # Use logger.info for a general success message
    logger.info(f"Attempting to add files to Git staging: {', '.join(files_to_commit)}")
    if not run_git_command(["git", "add"] + files_to_commit, "Git add successful for split batters files", "Git add failed for split batters files"):
        logger.error(f"❌ Aborting commit/push due to Git add failure for split batters files.")
        return # Exit if add fails

    # 2. Check if there are actual changes staged for commit
    # `git diff --cached --exit-code` without arguments checks the entire staging area
    # Returns 0 if no differences (nothing to commit), 1 if differences (changes staged)
    diff_check_result = subprocess.run(["git", "diff", "--cached", "--exit-code"], capture_output=True)

    if diff_check_result.returncode == 0:
        logger.warning(f"⚠️ No changes detected for split batters files to commit. Skipping commit/push.")
        # Optionally log debug output from git diff
        if diff_check_result.stdout: logger.debug(f"Git diff stdout (no changes):\n{diff_check_result.stdout.decode().strip()}")
        if diff_check_result.stderr: logger.debug(f"Git diff stderr (no changes):\n{diff_check_result.stderr.decode().strip()}")
    elif diff_check_result.returncode == 1:
        # Changes found, proceed with commit
        commit_msg = f"🔄 Split batters home/away @ {timestamp}"
        if run_git_command(["git", "commit", "-m", commit_msg], "Git commit successful for split batters files", "Git commit failed for split batters files", log_output=True):
            # 3. Push the commit
            # Remember, your YAML has a final push. If you want only one final push, comment this out.
            # If you want immediate pushes for each script, keep this.
            if run_git_command(["git", "push"], "Git push successful for split batters files", "Git push failed for split batters files"):
                logger.info(f"✅ Git commit and push successful: {commit_msg}")
            else:
                logger.error(f"❌ Git push failed for split batters files. See previous error for details.")
        else:
            logger.error(f"❌ Git commit failed for split batters files. See previous error for details.")
    else:
        # Any other unexpected return code from git diff
        logger.error(f"❌ Unexpected return code from git diff: {diff_check_result.returncode}. Cannot determine changes for split batters files.")
        if diff_check_result.stdout: logger.error(f"Git diff stdout:\n{diff_check_result.stdout.decode().strip()}")
        if diff_check_result.stderr: logger.error(f"Git diff stderr:\n{diff_check_result.stderr.decode().strip()}")

if __name__ == "__main__":
    main()
