# projection_formulas.py
import pandas as pd
from utils import safe_col

def calculate_projected_total_bases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 'projected_total_bases' for each batter.

    Args:
        df: DataFrame containing the necessary input columns.

    Returns:
        DataFrame with 'projected_total_bases' column added.
    """
    df_copy = df.copy()

    # --- ADD THESE DEBUG PRINTS ---
    print("\n--- DEBUG: Inside calculate_projected_total_bases ---")
    print(f"DEBUG: Columns available before calculation: {df_copy.columns.tolist()}")

    # List of columns used in the calculation
    input_cols = [
        "adj_woba_combined", "whiff%", "zone_swing_miss%",
        "out_of_zone_swing_miss%", "gb%", "fb%",
        "innings_pitched", "strikeouts"
    ]
    
    # Print head of relevant columns to see their values
    # Use safe_col to prevent errors if any of these are unexpectedly missing
    debug_df = pd.DataFrame()
    for col in input_cols:
        debug_df[col] = safe_col(df_copy, col, 0) # Use safe_col here too for printing
    debug_df['original_adj_woba_combined'] = df_copy.get('adj_woba_combined', pd.Series(0, index=df_copy.index)) # To see original before safe_col.
    print("\nDEBUG: First 5 rows of input values for total_bases calculation:")
    print(debug_df.head())
    print("--- END DEBUG: Inside calculate_projected_total_bases ---\n")
    # --- END ADDITIONS ---

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

# ... (other calculate_projected_X functions remain unchanged) ...

def calculate_all_projections(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates all batter projection metrics.
    """
    df = calculate_projected_total_bases(df)
    df = calculate_projected_hits(df)
    df = calculate_projected_walks(df)
    df = calculate_projected_singles(df)
    return df
