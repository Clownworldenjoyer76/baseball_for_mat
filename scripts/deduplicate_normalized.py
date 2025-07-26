import os
import pandas as pd
import unicodedata
import re
import subprocess
import logging

# --- Logging Setup ---
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
    name = re.sub(r"[^\w\s,\.]", "", name)
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

# --- Git Command Runner ---
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
        logger.error(f"❌ {error_prefix}: {' '.join(command_parts)} exited with {e.returncode}")
        if e.stdout:
            logger.error(f"Git stdout:\n{e.stdout.strip()}")
        if e.stderr:
            logger.error(f"Git stderr:\n{e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"❌ 'git' command not found. Is Git installed and in PATH?")
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
        logger.warning(f"⚠️ Input file not found: {path}. Skipping {label}.")
        continue

    logger.info(f"📦 Processing {label} from {path}")
    df = pd.read_csv(path)
    before = len(df)

    # Deduplicate
    df = df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
    after = len(df)
    logger.info(f"🧼 {label.capitalize()} deduplicated: {before} → {after}")

    # Team name reverse mapping
    try:
        team_map = pd.read_csv("data/Data/team_name_master.csv")[["team_name", "clean_team_name"]].dropna()
        reverse_map = dict(zip(team_map["clean_team_name"].str.strip(), team_map["team_name"].str.strip()))
        df["team"] = df["team"].astype(str).str.strip().replace(reverse_map)

        unmapped = df["team"].isna().sum()
        if unmapped > 0:
            logger.warning(f"⚠️ {unmapped} unmapped team(s) after applying reverse map.")
        else:
            logger.info(f"🔗 {label.capitalize()} team names mapped successfully.")
    except Exception as e:
        logger.warning(f"⚠️ Failed to map team names for {label}: {e}")

    output_path = f"{output_dir}/{label}_normalized_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Cleaned {label} data written to {output_path}")

    # --- Git Handling ---
    commit_message = f"🧹 Auto-cleaned and deduplicated {label}"

    if run_git_command(["git", "add", output_path], f"Git add OK: {output_path}", f"Git add failed: {output_path}"):
        diff_check = subprocess.run(["git", "diff", "--cached", "--exit-code", output_path], capture_output=True)
        if diff_check.returncode == 1:
            if run_git_command(["git", "commit", "-m", commit_message],
                               f"Git commit OK: {output_path}",
                               f"Git commit failed: {output_path}", log_output=True):
                if run_git_command(["git", "push"], f"Git push OK: {output_path}", f"Git push failed: {output_path}"):
                    logger.info(f"🚀 {output_path} committed and pushed.")
                else:
                    logger.error(f"❌ Git push failed for {output_path}")
            else:
                logger.error(f"❌ Git commit failed for {output_path}")
        elif diff_check.returncode == 0:
            logger.warning(f"⚠️ No changes to commit for {output_path}")
        else:
            logger.error(f"❌ Unexpected return code from git diff: {diff_check.returncode}")
    else:
        logger.error(f"❌ Git add failed: {output_path}")
