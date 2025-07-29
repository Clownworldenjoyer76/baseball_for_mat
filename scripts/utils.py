# utils.py
import pandas as pd
from pathlib import Path

def load_csv(path: Path) -> pd.DataFrame:
    """
    Safely loads a CSV file into a Pandas DataFrame and cleans column names.

    Args:
        path: The pathlib.Path object to the CSV file.

    Returns:
        A pandas.DataFrame containing the data from the CSV with cleaned column names.

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_csv(path)
    # Strip whitespace from column names to prevent KeyError due to invisible characters
    df.columns = df.columns.str.strip()
    return df

def safe_col(df: pd.DataFrame, col: str, default: int = 0) -> pd.Series:
    """
    Returns a Series for a given column from a DataFrame.
    If the column exists, it fills NaN values with a default.
    If the column does not exist, it creates a new Series filled with the default.

    Args:
        df: The pandas.DataFrame to check.
        col: The name of the column to retrieve.
        default: The default value to use if the column is missing or has NaNs.

    Returns:
        A pandas.Series for the specified column.
    """
    # Check if the column exists after stripping whitespace during load_csv
    return df[col].fillna(default) if col in df.columns else pd.Series([default] * len(df), index=df.index)

def standardize_name_key(df: pd.DataFrame, name_column: str) -> pd.DataFrame:
    """
    Adds a standardized 'name_key' column to a DataFrame.

    Args:
        df: The pandas.DataFrame to modify.
        name_column: The name of the column containing names to standardize.

    Returns:
        The DataFrame with the 'name_key' column added.
    """
    df_copy = df.copy() # Avoid modifying original DataFrame in place if it's reused
    df_copy["name_key"] = df_copy[name_column].astype(str).str.strip().str.lower()
    return df_copy


def standardize_name_key(df: pd.DataFrame, name_col: str = "last_name, first_name") -> pd.DataFrame:
    """
    Adds a 'name_key' column to the DataFrame, standardized for merge operations.
    Applies lowercasing, trimming, and accent stripping.
    """
    def strip_accents(text):
        return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

    import re
    def normalize(name):
        name = name.strip().lower()
        name = strip_accents(name)
        name = re.sub(r'[^a-z0-9, ]+', '', name)
        return name

    df['name_key'] = df[name_col].astype(str).apply(normalize)
    return df
