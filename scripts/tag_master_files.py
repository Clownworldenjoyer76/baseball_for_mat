import pandas as pd
import unicodedata
import re
from pathlib import Path
from datetime import datetime
import logging

# ─── Configuration ───────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

MASTER_FILE = Path("data/processed/player_team_master.csv")
BATTER_FILE = Path("data/normalized/batters_normalized.csv")
PITCHER_FILE = Path("data/normalized/pitchers_normalized.csv")
OUTPUT_FOLDER = Path("data/tagged")
OUTPUT_TOTALS_FILE = Path("data/output/player_totals.txt")

OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_TOTALS_FILE.parent.mkdir(parents=True, exist_ok=True)

RE_SUFFIX_REMOVE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?", re.IGNORECASE)
RE_NON_ALPHANUM_OR_SPACE_OR_COMMA = re.compile(r"[^\w\s,]")
RE_MULTI_SPACE = re.compile(r"\s+")

# ─── Name Normalization ──────────────────────────────────────

def normalize_name_series(names_series: pd.Series) -> pd.Series:
    names_series = names_series.astype(str).fillna("")
    names_series = names_series.str.normalize("NFD").str.encode("ascii", errors="ignore").str.decode("utf-8")
    names_series = names_series.str.replace("’", "", regex=False)
    names_series = names_series.str.replace("`", "", regex=False)
    names_series = names_series.str.replace(RE_NON_ALPHANUM_OR_SPACE_OR_COMMA, "", regex=True)
    names_series = names_series.str.replace(RE_MULTI_SPACE, " ", regex=True)
    names_series = names_series.str.strip()

    # Remove suffixes from the last name (before comma)
    def remove_suffix(name):
        parts = name.split(",", 1)
        if len(parts) == 2:
            last = RE_SUFFIX_REMOVE.sub("", parts[0]).strip().title()
            first = parts[1].strip().title()
            return f"{last}, {first}"
        return RE_SUFFIX_REMOVE.sub(name, "").strip().title()

    return names_series.apply(remove_suffix)

# ─── File Handling ───────────────────────────────────────────

def load_csv_safely(file_path: Path, column_to_check: str = None) -> pd.DataFrame:
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(file_path)
        if column_to_check and column_to_check not in df.columns:
            logging.warning(f"Column '{column_to_check}' not found in {file_path}.")
            return pd.DataFrame()
        return df
    except pd.errors.EmptyDataError:
        logging.warning(f"File is empty: {file_path}. Returning empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

def tag_and_save_players(input_file_path: Path, player_type: str, master_df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Tagging {player_type} data from {input_file_path}...")

    df_to_tag = load_csv_safely(input_file_path, "last_name, first_name")
    if df_to_tag.empty:
        logging.warning(f"Skipping {player_type} due to missing or invalid input.")
        return pd.DataFrame()

    df_to_tag["last_name, first_name"] = normalize_name_series(df_to_tag["last_name, first_name"])

    merged_df = df_to_tag.merge(
        master_df,
        how="left",
        left_on="last_name, first_name",
        right_on="name",
        suffixes=("", "_master")
    )

    unmatched = merged_df[merged_df["team"].isna() | merged_df["type"].isna()]
    if not unmatched.empty:
        examples = unmatched["last_name, first_name"].drop_duplicates().head(10).to_string(index=False)
        logging.warning(
            f"{len(unmatched)} unmatched {player_type} rows "
            f"({unmatched['last_name, first_name'].nunique()} unique). Examples:\n{examples}"
        )

    merged_clean = merged_df.dropna(subset=["team", "type"]).copy()

    key_cols = ["name", "player_id", "team", "type"]
    existing_key_cols = [col for col in key_cols if col in merged_clean.columns]
    ordered_cols = existing_key_cols + [col for col in merged_clean.columns if col not in existing_key_cols]
    merged_clean = merged_clean[ordered_cols]

    output_file_path = OUTPUT_FOLDER / input_file_path.name
    try:
        merged_clean.to_csv(output_file_path, index=False)
        logging.info(f"✅ Tagged {player_type} saved to: {output_file_path} ({len(merged_clean)} rows)")
    except Exception as e:
        logging.error(f"❌ Error saving tagged file: {e}")

    return merged_clean

# ─── Main ───────────────────────────────────────────────────

if __name__ == "__main__":
    logging.info("🚀 Starting player tagging process...")

    master_players_df = load_csv_safely(MASTER_FILE, "name")
    if master_players_df.empty:
        logging.critical("Master player file could not be loaded or is empty. Exiting.")
        exit(1)

    master_players_df["name"] = normalize_name_series(master_players_df["name"])

    all_tagged_dfs = {}
    for file_path, player_type in [(BATTER_FILE, "batters"), (PITCHER_FILE, "pitchers")]:
        all_tagged_dfs[player_type] = tag_and_save_players(file_path, player_type, master_players_df)

    try:
        with open(OUTPUT_TOTALS_FILE, "w") as f:
            f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            for key, df in all_tagged_dfs.items():
                f.write(f"Tagged {key.capitalize()}: {len(df)}\n")
        logging.info(f"📄 Summary written to {OUTPUT_TOTALS_FILE}")
    except Exception as e:
        logging.error(f"❌ Error writing summary: {e}")

    logging.info("✅ Tagging process complete.")
