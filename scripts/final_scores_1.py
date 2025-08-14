# scripts/final_props_1.py
import pandas as pd
from pathlib import Path

# ---------- Inputs / Outputs ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")
SCHED_FILE = Path("data/bets/mlb_sched.csv")
OUTPUT_FILE = Path("data/bets/player_props_history.csv")

# ---------- Required output columns ----------
OUT_COLS = [
    "player_id",
    "name",
    "team",
    "prop",
    "line",
    "value",
    "over_probability",
    "date",
    "game_id",
    "prop_correct",
    "prop_sort",
]

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def _coerce_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _first_existing_col(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _standardize_props(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure prop frames expose the core columns."""
    if df.empty:
        return df

    rename_map = {}
    if "prop_type" in df.columns and "prop" not in df.columns:
        rename_map["prop_type"] = "prop"
    if "player" in df.columns and "name" not in df.columns:
        rename_map["player"] = "name"
    if rename_map:
        df = df.rename(columns=rename_map)

    # ensure required cols exist
    for col in ["player_id", "name", "team", "prop", "line", "value",
                "over_probability", "date", "game_id"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = _coerce_numeric(df, ["line", "value", "over_probability"])
    # normalize strings
    for col in ["name", "team", "prop", "game_id"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # normalize date (keep as string YYYY-MM-DD where possible)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")

    return df[["player_id","name","team","prop","line","value",
               "over_probability","date","game_id"]].copy()

def _normalize_team_key(s: pd.Series) -> pd.Series:
    return s.astype("string").fillna("").str.strip().str.casefold()

def _standardize_schedule(df_sched: pd.DataFrame) -> pd.DataFrame:
    """Return schedule with columns: date, game_id, home_team, away_team, and keys."""
    if df_sched.empty:
        raise SystemExit("Schedule file is empty or missing.")

    df = df_sched.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    home_col = _first_existing_col(df, ["home_team", "home"])
    away_col = _first_existing_col(df, ["away_team", "away"])
    if not home_col or not away_col:
        raise SystemExit("Schedule must have home_team/away_team (or home/away).")

    date_col = _first_existing_col(df, ["date", "game_date"])
    if not date_col:
        raise SystemExit("Schedule must have a date column (date or game_date).")

    gid_col = _first_existing_col(df, ["game_id", "id", "gameid"])
    if not gid_col:
        # synthesize a game_id so pipeline always works
        df["game_id"] = (
            pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y%m%d").fillna("NA")
            + "_"
            + df[home_col].astype(str).str.replace(r"\s+", "", regex=True).str.lower()
            + "_"
            + df[away_col].astype(str).str.replace(r"\s+", "", regex=True).str.lower()
        )
        gid_col = "game_id"

    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col], errors="coerce").dt.date.astype("string"),
        "game_id": df[gid_col].astype("string"),
        "home_team": df[home_col].astype("string"),
        "away_team": df[away_col].astype("string"),
    })
    out["home_key"] = _normalize_team_key(out["home_team"])
    out["away_key"] = _normalize_team_key(out["away_team"])
    return out

def main():
    # --- Load inputs ---
    bat = _standardize_props(_read_csv(BATTER_FILE))
    pit = _standardize_props(_read_csv(PITCHER_FILE))
    sched = _standardize_schedule(_read_csv(SCHED_FILE))

    # combine props and prep keys
    props = pd.concat([bat, pit], ignore_index=True)
    props = props[props["over_probability"].notna()].copy()
    props["team_key"] = _normalize_team_key(props["team"])

    # container for per-game selections
    picked = []

    # iterate schedule; pick top-5 across both teams for each game
    for _, g in sched.iterrows():
        home_key, away_key = str(g["home_key"]), str(g["away_key"])
        candidates = props[props["team_key"].isin([home_key, away_key])].copy()
        if candidates.empty:
            continue

        # prefer same-date rows when present
        if candidates["date"].notna().any() and isinstance(g["date"], str):
            same_date = candidates["date"] == g["date"]
            if same_date.any():
                candidates = candidates[same_date]

        # order and take top 5
        candidates = candidates.sort_values("over_probability", ascending=False).head(5).copy()

        # inject schedule date/game_id if missing
        candidates["date"] = candidates["date"].where(candidates["date"].notna(), g["date"])
        candidates["game_id"] = candidates["game_id"].where(candidates["game_id"].notna(), g["game_id"])

        # prop_correct blank; prop_sort for ranks 1-3
        candidates["prop_correct"] = ""
        candidates["rank_in_game"] = range(1, len(candidates) + 1)
        candidates["prop_sort"] = candidates["rank_in_game"].apply(lambda r: "Best Prop" if r <= 3 else "game")

        picked.append(candidates[OUT_COLS])

    final_df = pd.concat(picked, ignore_index=True) if picked else pd.DataFrame(columns=OUT_COLS)

    # ensure all required columns exist
    for col in OUT_COLS:
        if col not in final_df.columns:
            final_df[col] = pd.NA

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved: {OUTPUT_FILE} rows={len(final_df)}")

if __name__ == "__main__":
    main()
