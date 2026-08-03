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

# --- Helper to locate usable columns in stadium_master.csv ---
def pick_column(df: pd.DataFrame, candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

# --- Main Deduplication Logic ---
files = {
    "batters": "data/tagged/batters_normalized.csv",
    "pitchers": "data/tagged/pitchers_normalized.csv"
}
output_dir = "data/cleaned"
os.makedirs(output_dir, exist_ok=True)

# Use the correct source file for team mapping
STADIUM_MASTER_PATH = "data/manual/stadium_master.csv"

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

    # Team name reverse mapping using stadium_master.csv
    try:
        if os.path.exists(STADIUM_MASTER_PATH):
            sm = pd.read_csv(STADIUM_MASTER_PATH)

            # Attempt to find appropriate columns to map back display team names
            # Prefer an explicit clean->display mapping if present
            display_col = pick_column(sm, ["team_name", "display_team_name", "team"])
            clean_col   = pick_column(sm, ["clean_team_name", "canonical_team_name", "team_clean", "clean_name"])
            abbr_col    = pick_column(sm, ["team_abbr", "abbr", "code"])

            reverse_map = None

            if clean_col and display_col:
                # Map from clean -> display
                tmp = sm[[clean_col, display_col]].dropna()
                reverse_map = dict(zip(tmp[clean_col].astype(str).str.strip(),
                                       tmp[display_col].astype(str).str.strip()))
            elif abbr_col and display_col:
                # Fallback: map from abbreviation -> display name
                tmp = sm[[abbr_col, display_col]].dropna()
                reverse_map = dict(zip(tmp[abbr_col].astype(str).str.strip(),
                                       tmp[display_col].astype(str).str.strip()))

            if reverse_map:
                df["team"] = df["team"].astype(str).str.strip().replace(reverse_map)
                unmapped = df["team"].isna().sum()
                if unmapped > 0:
                    logger.warning(f"⚠️ {unmapped} unmapped team(s) after applying reverse map from stadium_master.csv.")
                else:
                    logger.info(f"🔗 {label.capitalize()} team names mapped using stadium_master.csv.")
            else:
                logger.warning("⚠️ Could not determine suitable columns in stadium_master.csv for team mapping; leaving 'team' values as-is.")
        else:
            logger.warning(f"⚠️ Mapping source not found: {STADIUM_MASTER_PATH}; leaving 'team' values as-is.")
    except Exception as e:
        logger.warning(f"⚠️ Failed to map team names for {label} using stadium_master.csv: {e}")

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
