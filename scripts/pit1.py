# scripts/pit1.py

import pandas as pd
from pathlib import Path

# Inputs
HWP_FILE   = Path("data/end_chain/first/pit_hwp.csv")
AWP_FILE   = Path("data/end_chain/first/pit_awp.csv")
XTRA_FILE  = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

# Output
OUTPUT_FILE = Path("data/end_chain/final/startingpitchers.csv")

ID_CANDIDATES = ["player_id", "pitcher_id", "mlb_id"]

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def coerce_player_id(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Ensure a 'player_id' column exists; try common alternatives."""
    cols = set(df.columns)
    found = next((c for c in ID_CANDIDATES if c in cols), None)
    if found is None:
        raise KeyError(f"{name}: no id column found (tried {ID_CANDIDATES}). Columns: {sorted(cols)[:25]}...")
    if found != "player_id":
        df = df.rename(columns={found: "player_id"})
    # keep as string to avoid 64-bit/float issues
    df["player_id"] = df["player_id"].astype("string")
    return df

def main():
    # Load home/away starters
    hwp = load_csv(HWP_FILE)
    awp = load_csv(AWP_FILE)

    hwp = coerce_player_id(hwp, "pit_hwp")
    awp = coerce_player_id(awp, "pit_awp")

    # Tag context and align team column names if needed
    hwp["team_context"] = "home"
    awp["team_context"] = "away"

    if "home_team" in hwp.columns and "team" not in hwp.columns:
        hwp = hwp.rename(columns={"home_team": "team"})
    if "away_team" in awp.columns and "team" not in awp.columns:
        awp = awp.rename(columns={"away_team": "team"})

    # Combine today’s pitchers (expect 1 home + 1 away per game, but be flexible)
    today_pitchers = pd.concat([hwp, awp], ignore_index=True)

    # Make sure player_id is string for all inputs
    today_pitchers["player_id"] = today_pitchers["player_id"].astype("string")

    # Load extra stats and coerce ID
    xtra = load_csv(XTRA_FILE)
    xtra = coerce_player_id(xtra, "pitchers_xtra_normalized")

    # If extra file happens to have a 'name' column, keep it but avoid duplicate conflicts
    drop_dupes = []
    for c in ["team"]:  # common overlap from xtra; keep today's 'team' if already present
        if c in xtra.columns and c in today_pitchers.columns:
            drop_dupes.append(c)
    xtra_merge = xtra.drop(columns=drop_dupes, errors="ignore")

    # Merge on player_id only; suffix xtra cols to make provenance obvious
    merged = pd.merge(
        today_pitchers,
        xtra_merge,
        on="player_id",
        how="left",
        suffixes=("", "_xtra")
    )

    # Simple diagnostics
    total_rows = len(merged)
    missing_xtra = merged["player_id"][merged.filter(like="_xtra").isna().all(axis=1)].nunique()
    unique_sp = merged["player_id"].nunique()

    print(f"🧩 Combined starters: {unique_sp} unique player_id (rows={total_rows})")
    print(f"ℹ️  Starters with no xtra match: {missing_xtra}")

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Merged pitcher data saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
