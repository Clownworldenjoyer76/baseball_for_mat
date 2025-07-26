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

# ─── Name Utilities ──────────────────────────────────────────

def strip_accents(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name_series(names_series: pd.Series) -> pd.Series:
    names_series = names_series.astype(str).fillna("")
    names_series = names_series.apply(strip_accents)
    names_series = names_series.str.replace("’", "", regex=False)
    names_series = names_series.str.replace("`", "", regex=False)
    names_series = names_series.str.replace(RE_NON_ALPHANUM_OR_SPACE_OR_COMMA, "", regex=True)
    names_series = names_series.str.replace(RE_MULTI_SPACE, " ", regex=True)
    names_series = names_series.str.strip()

    name_parts_df = pd.DataFrame(index=names_series.index)
    name_parts_df["raw_name"] = names_series

    has_comma_mask = name_parts_df["raw_name"].str.contains(",", na=False)

    commas_split = name_parts_df.loc[has_comma_mask, "raw_name"].str.split(",", n=1, expand=True)
    if not commas_split.empty and commas_split.shape[1] == 2:
        name_parts_df.loc[has_comma_mask, "last"] = commas_split[0].str.strip()
        name_parts_df.loc[has_comma_mask, "first"] = commas_split[1].str.strip()

    no_comma_names = name_parts_df.loc[~has_comma_mask, "raw_name"]
    space_split = no_comma_names.str.split(" ", n=1, expand=True)

    if not space_split.empty:
        name_parts_df.loc[~has_comma_mask, "first"] = space_split[0].str.strip()
        if space_split.shape[1] > 1:
            name_parts_df.loc[~has_comma_mask, "last"] = space_split[1].str.strip()
        else:
            name_parts_df.loc[~has_comma_mask, "last"] = ""

    name_parts_df["last"] = name_parts_df["last"].fillna("").str.title()
    name_parts_df["first"] = name_parts_df["first"].fillna("").str.title()
    name_parts_df["last"] = name_parts_df["last"].apply(lambda x: RE_SUFFIX_REMOVE.sub("", x).strip())

    return name_parts_df.apply(
        lambda row: f"{row['last']}, {row['first']}" if row['first'] else row['last'],
        axis=1
    )

# ─── Load & Tag ──────────────────────────────────────────────

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
        logging.warning(f"Skipping {player_type} due to issues loading or missing column in {input_file_path}.")
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
        unique_unmatched_names = unmatched["last_name, first_name"].drop_duplicates()
        logging.warning(
            f"{len(unmatched)} {player_type} rows ({len(unique_unmatched_names)} unique names) "
            f"had no team/type match and will be dropped. Examples:\n"
            f"{unique_unmatched_names.to_string(index=False)}"
        )

    merged_clean = merged_df.dropna(subset=["team", "type"]).copy()

    if merged_clean.empty:
        logging.warning(f"No {player_type} rows matched after tagging. Output file will be empty.")
        return pd.DataFrame()

    key_cols = ["name", "player_id", "team", "type"]
    existing_key_cols = [col for col in key_cols if col in merged_clean.columns]
    other_cols = [col for col in merged_clean.columns if col not in existing_key_cols]
    merged_clean = merged_clean[existing_key_cols + other_cols]

    output_file_path = OUTPUT_FOLDER / input_file_path.name
    try:
        merged_clean.to_csv(output_file_path, index=False)
        logging.info(f"✅ Tagged {player_type} saved to: {output_file_path} ({len(merged_clean)} rows)")
    except Exception as e:
        logging.error(f"Error saving tagged {player_type} data to {output_file_path}: {e}")
        return pd.DataFrame()

    return merged_clean

# ─── Main ───────────────────────────────────────────────────

if __name__ == "__main__":
    logging.info("Starting player tagging process...")

    master_players_df = load_csv_safely(MASTER_FILE, "name")
    if master_players_df.empty:
        logging.critical("Master player file could not be loaded or is empty. Exiting.")
        exit(1)

    master_players_df["name"] = normalize_name_series(master_players_df["name"])

    all_tagged_dfs = {}
    for file_path, player_type in [(BATTER_FILE, "batters"), (PITCHER_FILE, "pitchers")]:
        tagged_df = tag_and_save_players(file_path, player_type, master_players_df)
        all_tagged_dfs[player_type] = tagged_df

    try:
        with open(OUTPUT_TOTALS_FILE, "w") as f:
            f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            for player_type, df in all_tagged_dfs.items():
                f.write(f"Tagged {player_type.capitalize()}: {len(df)}\n")
        logging.info(f"Summary totals written to {OUTPUT_TOTALS_FILE}")
    except Exception as e:
        logging.error(f"Error writing totals summary to {OUTPUT_TOTALS_FILE}: {e}")

    logging.info("Player tagging process completed.")
