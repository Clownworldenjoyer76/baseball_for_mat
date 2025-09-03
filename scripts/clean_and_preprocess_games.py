#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Inputs/Outputs
RAW_GAMES   = Path("data/end_chain/first/games_today.csv")          # produced earlier in 04a
CLEAN_OUT   = Path("data/end_chain/cleaned/games_today_cleaned.csv")
MLB_IDS_CSV = Path("data/raw/mlb_game_ids.csv")                     # optional enrichment

def _key(s: str) -> str:
    """Simple alnum-lowered key for joining."""
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def main():
    if not RAW_GAMES.exists():
        raise SystemExit(f"❌ missing input: {RAW_GAMES}")

    g = pd.read_csv(RAW_GAMES)
    g.columns = g.columns.str.strip()

    # Ensure required team columns present
    rename_map = {
        "Home": "home_team", "home": "home_team",
        "Away": "away_team", "away": "away_team"
    }
    for src, dst in rename_map.items():
        if src in g.columns and dst not in g.columns:
            g = g.rename(columns={src: dst})

    needed = {"home_team", "away_team"}
    if not needed.issubset(g.columns):
        missing = sorted(needed - set(g.columns))
        raise SystemExit(f"❌ games_today missing columns: {missing}")

    # Preserve existing identifiers/fields if present
    has_game_id   = "game_id"   in g.columns
    has_game_time = "game_time" in g.columns

    # Keys for optional MLB ID join
    g["home_key"] = g["home_team"].apply(_key)
    g["away_key"] = g["away_team"].apply(_key)

    # Optional enrichment with MLB game ids
    if MLB_IDS_CSV.exists():
        ids = pd.read_csv(MLB_IDS_CSV)
        ids = ids.rename(columns={"date": "mlb_date"})
        ids["home_key"] = ids["home_team"].apply(_key)
        ids["away_key"] = ids["away_team"].apply(_key)

        g = g.merge(
            ids[["mlb_date", "home_key", "away_key", "game_pk", "game_number", "game_datetime"]],
            on=["home_key", "away_key"],
            how="left"
        )
        # If we already have a game_id, keep it; else use game_pk from MLB IDs
        if "game_pk" in g.columns:
            g["game_id"] = g.get("game_id").combine_first(g["game_pk"]).astype("Int64").astype("string")
    else:
        # No enrichment: DO NOT overwrite existing game_id
        if not has_game_id:
            g["game_id"] = pd.NA

    # Output ordering: keep core identifiers first, preserve all other columns
    preferred = ["game_id", "home_team", "away_team", "game_datetime", "game_number"]
    # If we had game_time initially, keep it near the front as well
    if has_game_time and "game_time" not in preferred:
        preferred.insert(3, "game_time")

    # Build final column order
    existing = [c for c in preferred if c in g.columns]
    others   = [c for c in g.columns if c not in existing]
    out = g[existing + others]

    CLEAN_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(CLEAN_OUT, index=False)
    print(f"✅ Saved cleaned game data to: {CLEAN_OUT}")

if __name__ == "__main__":
    main()
