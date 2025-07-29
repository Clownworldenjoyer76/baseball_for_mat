# projection_formulas.py
import pandas as pd
from .utils import safe_col # Assuming utils.py is in the same directory

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 'projected_total_bases' for each batter.

    Args:
        df: DataFrame containing the necessary input columns.

    Returns:
        DataFrame with 'projected_total_bases' column added.
    """
    df_copy = df.copy() # Work on a copy

    df_copy["projected_total_bases"] = (
        safe_col(df_copy, "adj_woba_combined", 0) * 1.75 +
        safe_col(df_copy, "whiff%", 0) * -0.1 +
        safe_col(df_copy, "zone_swing_miss%", 0) * -0.05 +
        safe_col(df_copy, "out_of_zone_swing_miss%", 0) * -0.05 +
        safe_col(df_copy, "gb%", 0) * -0.02 +
        safe_col(df_copy, "fb%", 0) * 0.03 +
        safe_col(df_copy, "innings_pitched", 0) * -0.01 +
        safe_col(df_copy, "strikeouts", 0) * 0.005
    ).round(2)
    return df_copy

def calculate_projected_hits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 'projected_hits' for each batter.

    Args:
        df: DataFrame containing the necessary input columns.

    Returns:
        DataFrame with 'projected_hits' column added.
    """
    df_copy = df.copy()
    df_copy["projected_hits"] = safe_col(df_copy, "hit", 0).round(2)
    return df_copy

def calculate_projected_walks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 'projected_walks' for each batter.

    Args:
        df: DataFrame containing the necessary input columns.

    Returns:
        DataFrame with 'projected_walks' column added.
    """
    df_copy = df.copy()
    df_copy["projected_walks"] = safe_col(df_copy, "bb_percent", 0).round(2)
    return df_copy

def calculate_projected_singles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 'projected_singles' for each batter.

    Args:
        df: DataFrame containing the necessary input columns.

    Returns:
        DataFrame with 'projected_singles' column added.
    """
    df_copy = df.copy()
    df_copy["projected_singles"] = (
        df_copy["projected_hits"] -
        safe_col(df_copy, "double", 0) -
        safe_col(df_copy, "triple", 0) -
        safe_col(df_copy, "home_run", 0)
    ).clip(lower=0).round(2)
    return df_copy

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates all batter projection metrics.

    Args:
        df: DataFrame containing the preprocessed batter data.

    Returns:
        DataFrame with all projected stats added.
    """
    df = calculate_projected_total_bases(df)
    df = calculate_projected_hits(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_singles(df)
    return df

