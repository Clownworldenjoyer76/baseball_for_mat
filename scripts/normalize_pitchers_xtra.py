import pandas as pd
from pathlib import Path

# ---- Paths ----
SRC_PITCHERS = Path("data/Data/pitchers.csv")
TODAY_GAMES  = Path("data/raw/todaysgames_normalized.csv")
OUT_DIR      = Path("data/end_chain/cleaned")
OUT_FILE     = OUT_DIR / "pitchers_xtra_normalized.csv"

def _coerce_id_series(s: pd.Series) -> pd.Series:
    """
    Coerce an ID-like series to pandas nullable Int64 consistently.
    Handles floats/strings/NaNs safely.
    """
    # Try numeric first, then to Int64
    s_num = pd.to_numeric(s, errors="coerce")
    return s_num.astype("Int64")

def main():
    # --- Load source pitchers ---
    if not SRC_PITCHERS.exists():
        raise SystemExit(f"❌ Missing source file: {SRC_PITCHERS}")
    df = pd.read_csv(SRC_PITCHERS)

    # Ensure player_id exists
    if "player_id" not in df.columns:
        raise SystemExit("❌ 'player_id' column missing in data/Data/pitchers.csv")

    # --- Column renames (only if present) ---
    rename_map = {
        "p_formatted_ip": "innings_pitched",
        "strikeout": "strikeouts",
        "walk": "walks",
        "p_earned_run": "earned_runs",
    }
    existing_map = {k: v for k, v in rename_map.items() if k in df.columns}
    if existing_map:
        df = df.rename(columns=existing_map)

    # Normalize pitcher player_id dtype for safe merging
    df["player_id"] = _coerce_id_series(df["player_id"])

    # --- Load today's games to filter and attach team_id ---
    if not TODAY_GAMES.exists():
        raise SystemExit(f"❌ Missing games file: {TODAY_GAMES}")
    g = pd.read_csv(TODAY_GAMES)

    # Validate required columns in todaysgames_normalized.csv
    required_cols = {
        "pitcher_home_id", "pitcher_away_id",
        "home_team_id", "away_team_id"
    }
    missing = sorted(required_cols - set(g.columns))
    if missing:
        raise SystemExit(f"❌ {TODAY_GAMES} missing columns: {missing}")

    # Coerce id columns to Int64
    g["pitcher_home_id"] = _coerce_id_series(g["pitcher_home_id"])
    g["pitcher_away_id"] = _coerce_id_series(g["pitcher_away_id"])
    g["home_team_id"]    = _coerce_id_series(g["home_team_id"])
    g["away_team_id"]    = _coerce_id_series(g["away_team_id"])

    # --- Step 3: Filter pitchers to only those appearing today (home or away) ---
    todays_ids = pd.concat([
        g["pitcher_home_id"].dropna(),
        g["pitcher_away_id"].dropna()
    ], ignore_index=True).dropna().unique()

    df = df[df["player_id"].isin(todays_ids)].copy()

    # --- Step 4: Insert team_id by matching player_id from todaysgames_normalized ---
    # Build mapping from player_id → team_id using both home and away roles
    home_map = g.loc[g["pitcher_home_id"].notna(), ["pitcher_home_id", "home_team_id"]].rename(
        columns={"pitcher_home_id": "player_id", "home_team_id": "team_id"}
    )
    away_map = g.loc[g["pitcher_away_id"].notna(), ["pitcher_away_id", "away_team_id"]].rename(
        columns={"pitcher_away_id": "player_id", "away_team_id": "team_id"}
    )
    pid_team = pd.concat([home_map, away_map], ignore_index=True).dropna().drop_duplicates(subset=["player_id"])

    # Merge to add team_id
    df = df.merge(pid_team, on="player_id", how="left")

    # Ensure output directory and write
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(df):,} rows → {OUT_FILE}")

if __name__ == "__main__":
    main()
