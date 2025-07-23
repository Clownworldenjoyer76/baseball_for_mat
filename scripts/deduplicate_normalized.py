import os
import pandas as pd
import unicodedata
import re
import subprocess
import logging

# --- Logging Setup ---
# Configure logging to output to stdout/stderr so it gets captured by the YAML's &>> redirection.
# Set level to INFO for general messages, DEBUG for more verbose Git command outputs.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Normalization Functions ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in text if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""

    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)  # Keep alphanumerics, comma, period
    name = re.sub(r"\s+", " ", name).strip()

    if "," not in name:
        tokens = name.split()
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return name.title()

    parts = name.split(",")
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().title()
        return f"{last}, {first}"

    return name.title()

# --- Helper function for running Git commands ---
# This helper will print stdout/stderr from subprocess.run for better debugging.
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

# --- Main Deduplication Logic ---
files = {
    "batters": "data/tagged/batters_normalized.csv",
    "pitchers": "data/tagged/pitchers_normalized.csv"
}
output_dir = "data/cleaned"
os.makedirs(output_dir, exist_ok=True)

for label, path in files.items():
    if not os.path.exists(path):
        logger.warning(f"⚠️ Input file not found: {path}. Skipping {label} deduplication.")
        continue

    logger.info(f"Processing {label} from {path}")
    df = pd.read_csv(path)
    before = len(df)

    # Normalize names before deduplication
    df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
    df = df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
    after = len(df)

    logger.info(f"🧼 {label.capitalize()} deduplicated: {before} → {after}")

    # Map team names to official casing using team_name_master
    try:
        # Assuming team_name_master.csv is in data/Data relative to the script's execution dir
        team_map = pd.read_csv("data/Data/team_name_master.csv")
        team_map = team_map[['team_name', 'clean_team_name']].dropna()
        reverse_map = dict(zip(team_map['clean_team_name'].str.strip(), team_map['team_name'].str.strip()))
        df['team'] = df['team'].astype(str).str.strip().map(reverse_map)
        logger.info(f"🔗 {label.capitalize()} team names mapped using team_name_master.csv")
    except Exception as e:
        logger.warning(f"⚠️ Failed to map team names for {label} using team_name_master.csv: {e}")

    output_path = f"{output_dir}/{label}_normalized_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Wrote cleaned {label} data to {output_path}")

    # --- Git commit logic for output file ---
    commit_message = f"🧹 Auto-cleaned and deduplicated {label}"

    # 1. Add the file to the staging area
    if run_git_command(["git", "add", output_path], f"Git add successful for {output_path}", f"Git add failed for {output_path}"):
        # 2. Check if there are actual changes staged for commit
        # `git diff --cached --exit-code <file>` returns:
        #   0 if no differences
        #   1 if differences (i.e., file is changed and staged)
        # We only want to commit if there are actual differences.
        diff_check = subprocess.run(["git", "diff", "--cached", "--exit-code", output_path], capture_output=True)

        if diff_check.returncode == 0:
            logger.warning(f"⚠️ No changes detected for {output_path} to commit after deduplication. Skipping commit/push.")
            # Log any output from diff_check for debugging, even if it's empty
            if diff_check.stdout:
                logger.debug(f"Git diff stdout (no changes): {diff_check.stdout.decode().strip()}")
            if diff_check.stderr:
                logger.debug(f"Git diff stderr (no changes): {diff_check.stderr.decode().strip()}")

        elif diff_check.returncode == 1:
            # Changes found, proceed with commit
            if run_git_command(["git", "commit", "-m", commit_message], f"Git commit successful for {output_path}", f"Git commit failed for {output_path}", log_output=True):
                # 4. Push the commit
                # As noted before, your YAML workflow has a final commit/push step for all logs.
                # If you want this specific file's changes pushed immediately, keep this `git push`.
                # If you prefer a single final push in the YAML, you can comment this `git push` out.
                # For now, let's keep it consistent with your original intent to push here.
                if run_git_command(["git", "push"], f"Git push successful for {output_path}", f"Git push failed for {output_path}"):
                    logger.info(f"✅ Successfully committed and pushed {output_path}")
                else:
                    logger.error(f"❌ Git push failed for {output_path}. Check previous error for details.")
            else:
                logger.error(f"❌ Git commit failed for {output_path}. Check previous error for details.")
        else:
            # Any other unexpected return code from git diff
            logger.error(f"❌ Unexpected return code from git diff ({diff_check.returncode}). Cannot determine changes for {output_path}.")
            if diff_check.stdout:
                logger.error(f"Git diff stdout:\n{diff_check.stdout.decode().strip()}")
            if diff_check.stderr:
                logger.error(f"Git diff stderr:\n{diff_check.stderr.strip()}")
    else:
        logger.error(f"❌ Git add failed for {output_path}. Cannot proceed with commit/push.")

