# scripts/normalize_names.py

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
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""

    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    suffixes = {"Jr", "Sr", "II", "III", "IV", "V", "Jr.", "Sr."}
    name = name.replace(",", "").strip()
    tokens = name.split()

    if len(tokens) < 2:
        return name.title()

    if tokens[-1] in suffixes:
        last = " ".join(tokens[:-2] + [tokens[-1]])
        first = tokens[-2]
    else:
        last = tokens[-2]
        first = tokens[-1]

    return f"{last.strip().title()}, {first.strip().title()}"

# --- Main Processing ---
def normalize_file(input_path, output_path):
    if not os.path.exists(input_path):
        logger.error(f"Missing file: {input_path}")
        return

    df = pd.read_csv(input_path)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
    elif "name" in df.columns:
        df["name"] = df["name"].apply(normalize_name)
    else:
        logger.warning(f"No name column found in {input_path}")
        return

    df.to_csv(output_path, index=False)
    logger.info(f"✅ Normalized: {input_path} → {output_path}")

    subprocess.run(["git", "add", output_path])
    subprocess.run(["git", "commit", "-m", f"Normalize names in {os.path.basename(output_path)}"])
    subprocess.run(["git", "push"])

def main():
    normalize_file("data/Data/batters.csv", "data/normalized/batters_normalized.csv")
    normalize_file("data/Data/pitchers.csv", "data/normalized/pitchers_normalized.csv")

if __name__ == "__main__":
    main()
