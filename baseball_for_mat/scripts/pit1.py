# pit1.py  (merge on player_id)

import pandas as pd
from pathlib import Path

# Inputs
HWP_FILE   = Path("data/end_chain/first/pit_hwp.csv")
AWP_FILE   = Path("data/end_chain/first/pit_awp.csv")
XTRA_FILE  = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/end_chain/final/startingpitchers.csv")

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    # Read as strings to avoid any ID coercion to float
    return pd.read_csv(path, dtype={"player_id": "string"}, low_memory=False)

def normalize_team_value(v):
    return "" if pd.isna(v) else str(v).strip()

def main():
    # --- Load inputs ---
    hwp = load_csv(HWP_FILE)
    awp = load_csv(AWP_FILE)
    xtra = load_csv(XTRA_FILE)

    # --- Basic column checks ---
    required_awh_cols = ["player_id"]
    for col in required_awh_cols:
        if col not in hwp.columns or col not in awp.columns:
            raise KeyError(f"'{col}' must exist in both pit_hwp.csv and pit_awp.csv")

    if "player_id" not in xtra.columns:
        raise KeyError("'player_id' must exist in pitchers_xtra_normalized.csv")

    # Ensure string merge key across all dfs
    hwp["player_id"] = hwp["player_id"].astype("string")
    awp["player_id"] = awp["player_id"].astype("string")
    xtra["player_id"] = xtra["player_id"].astype("string")

    # --- Tag context & align team column names for today’s pitchers ---
    hwp = hwp.copy()
    awp = awp.copy()
    hwp["team_context"] = "home"
    awp["team_context"] = "away"

    # Standardize 'team' column for both
    if "home_team" in hwp.columns:
        hwp.rename(columns={"home_team": "team"}, inplace=True)
    if "away_team" in awp.columns:
        awp.rename(columns={"away_team": "team"}, inplace=True)

    # Normalize 'team' text a bit
    if "team" in hwp.columns:
        hwp["team"] = hwp["team"].map(normalize_team_value)
    if "team" in awp.columns:
        awp["team"] = awp["team"].map(normalize_team_value)

    # --- Combine today's pitchers (home + away) ---
    today_pitchers = pd.concat([hwp, awp], ignore_index=True)

    # --- Prepare xtra: drop obviously redundant columns (optional/defensive) ---
    # Keep player_id as the key; if xtra has a plain 'name' column we won’t need it after merge
    xtra = xtra.copy()
    drop_candidates = [c for c in ["name", "last_name, first_name", "year"] if c in xtra.columns]
    if drop_candidates:
        xtra.drop(columns=drop_candidates, inplace=True, errors="ignore")

    # --- Merge on player_id (left join keeps all today’s pitchers) ---
    merged = pd.merge(
        today_pitchers,
        xtra,
        on="player_id",
        how="left",
        suffixes=("", "_xtra"),
    )

    # --- Clean duplicates created by prior flows (e.g., duplicate name/year columns) ---
    # Drop any exact “.1” columns (pandas’ default when same-name columns already existed before).
    dup_dot_one = [c for c in merged.columns if c.endswith(".1")]
    if dup_dot_one:
        merged.drop(columns=dup_dot_one, inplace=True, errors="ignore")

    # If xtra brought a second 'team' (as 'team_xtra'), prefer today's 'team' but fill missing from xtra
    if "team_xtra" in merged.columns and "team" in merged.columns:
        merged["team"] = merged["team"].where(merged["team"].ne(""), merged["team_xtra"])
        merged.drop(columns=["team_xtra"], inplace=True)

    # Optional: de-duplicate rows if any accidental duplication by player_id
    merged.drop_duplicates(subset=["player_id", "team_context"], inplace=True)

    # --- Save ---
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Merged pitcher data saved to: {OUTPUT_FILE}")

    # --- Quick summary ---
    matched = merged["player_id"].notna() & merged.drop(columns=today_pitchers.columns, errors="ignore").any(axis=1)
    print(f"ℹ️ Rows: {len(merged)} | Matched to xtra by player_id (has any xtra data): {matched.sum()}")

if __name__ == "__main__":
    main()
