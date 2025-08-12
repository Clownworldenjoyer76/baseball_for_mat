#!/usr/bin/env python3
# scripts/attach_pitcher_ids.py
# Add home_pitcher_id and away_pitcher_id to today's games by joining to pitchers_normalized.
# Deterministic normalization, then fail-fast report if any starters remain unmapped.

import sys
import unicodedata
import pandas as pd
from pathlib import Path

GAMES_CANDIDATES = [
    Path("data/cleaned/games_today_cleaned.csv"),
    Path("data/end_chain/cleaned/games_today_cleaned.csv"),
    Path("data/raw/todaysgames_normalized.csv"),
]
PITCHERS_FILE = Path("data/tagged/pitchers_normalized.csv")

def _pick_first_existing(paths):
    for p in paths:
        if p.exists():
            return p
    return None

def _require_file(p: Path, label: str):
    if not p.exists():
        print(f"❌ Required file missing: {label} → {p}", file=sys.stderr)
        sys.exit(1)

def _norm_name(s: str) -> str:
    s = (s or "").strip()
    # strip accents
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = s.lower()
    # remove punctuation and extra spaces
    for ch in [",", ".", "'", '"']:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    # drop common suffixes
    toks = [t for t in s.split() if t not in {"jr", "sr", "ii", "iii", "iv"}]
    s = " ".join(toks)
    return s

def _as_first_last(s: str) -> str:
    # support "last, first" -> "first last"
    raw = (s or "").strip()
    if "," in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}"
    return raw

def main():
    games_path = _pick_first_existing(GAMES_CANDIDATES)
    _require_file(PITCHERS_FILE, "pitchers_normalized")
    if games_path is None:
        print("❌ No games file found in expected locations.", file=sys.stderr)
        sys.exit(1)

    games = pd.read_csv(games_path)
    print(f"🧾 Using games file: {games_path} (rows={len(games)})")

    # Resolve pitcher name columns
    name_opts_home = ["home_pitcher","pitcher_home","home_sp","home_starter"]
    name_opts_away = ["away_pitcher","pitcher_away","away_sp","away_starter"]
    def _pick_col(df, options):
        for c in options:
            if c in df.columns:
                return c
        lower = {c.lower(): c for c in df.columns}
        for c in options:
            if c.lower() in lower:
                return lower[c.lower()]
        return None

    hp_col = _pick_col(games, name_opts_home)
    ap_col = _pick_col(games, name_opts_away)
    if not (hp_col and ap_col):
        print("❌ Missing home/away pitcher name columns in games file.", file=sys.stderr)
        print(f"   looked for home in {name_opts_home} and away in {name_opts_away}", file=sys.stderr)
        sys.exit(1)

    # Build pitcher name → id map from pitchers_normalized
    pits = pd.read_csv(PITCHERS_FILE)
    # prefer 'last_name, first_name' then 'name'
    nm_col = "last_name, first_name" if "last_name, first_name" in pits.columns else ("name" if "name" in pits.columns else None)
    if nm_col is None or "player_id" not in pits.columns:
        print("❌ pitchers_normalized must include 'player_id' and a name column ('last_name, first_name' or 'name').", file=sys.stderr)
        sys.exit(1)

    pits["_name_first_last"] = pits[nm_col].apply(_as_first_last)
    pits["_key"] = pits["_name_first_last"].apply(_norm_name)
    pits["player_id"] = pits["player_id"].astype(str).str.strip()

    name_to_id = pits.dropna(subset=["_key","player_id"]).drop_duplicates("_key").set_index("_key")["player_id"]

    # Create lookup keys for games
    games["_home_key"] = games[hp_col].astype(str).apply(_as_first_last).apply(_norm_name)
    games["_away_key"] = games[ap_col].astype(str).apply(_as_first_last).apply(_norm_name)

    games["home_pitcher_id"] = games["_home_key"].map(name_to_id)
    games["away_pitcher_id"] = games["_away_key"].map(name_to_id)

    miss_home = games["home_pitcher_id"].isna().sum()
    miss_away = games["away_pitcher_id"].isna().sum()
    if miss_home or miss_away:
        sample_home = games[games["home_pitcher_id"].isna()][hp_col].head(5).tolist()
        sample_away = games[games["away_pitcher_id"].isna()][ap_col].head(5).tolist()
        print(f"⚠️ Unmatched starters → home: {miss_home}, away: {miss_away}")
        if sample_home: print(f"   e.g. home samples: {sample_home}")
        if sample_away: print(f"   e.g. away samples: {sample_away}")

    # Write back in-place
    games.to_csv(games_path, index=False)
    print(f"✅ Wrote IDs into {games_path} (home_pitcher_id, away_pitcher_id)")

if __name__ == "__main__":
    main()
