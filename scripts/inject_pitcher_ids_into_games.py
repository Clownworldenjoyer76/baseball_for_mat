#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/inject_pitcher_ids_into_games.py
# Resolve pitcher IDs for today's games; if unresolved, assign numeric placeholders
# so downstream steps don't drop games. Also log any placeholders used.

import pandas as pd
from pathlib import Path
import glob

GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
TEAM_PITCHERS_GLOB = "data/team_csvs/pitchers_*.csv"
LOG_FILE = Path("summaries/foundation/missing_pitcher_ids.txt")

# Hard overrides (authoritative)
OVERRIDES = {
    "Richardson, Simeon Woods": 680573,
    "Gipson-Long, Sawyer": 687830,
    "Berríos, José": 621244,
}

def _to_int64(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def load_games() -> pd.DataFrame:
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"{GAMES_FILE} not found")
    df = pd.read_csv(GAMES_FILE, dtype=str).fillna("")
    req = {"game_id", "home_team", "away_team", "pitcher_home", "pitcher_away", "home_team_id", "away_team_id"}
    missing = req - set(df.columns)
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing)}")

    # Ensure id columns exist for passthrough
    for col in ("pitcher_home_id", "pitcher_away_id"):
        if col not in df.columns:
            df[col] = pd.NA

    # Normalize key numeric ids to Int64 (nullable)
    for c in ("game_id", "home_team_id", "away_team_id", "pitcher_home_id", "pitcher_away_id"):
        df[c] = _to_int64(df[c])

    return df

def build_name_to_id() -> dict:
    name_to_id: dict[str, int] = {}

    # 1) master file
    if MASTER_FILE.exists():
        mf = pd.read_csv(MASTER_FILE)
        if {"name", "player_id"}.issubset(mf.columns):
            mf = mf.dropna(subset=["name", "player_id"])
            for _, r in mf.iterrows():
                try:
                    pid = int(pd.to_numeric(r["player_id"], errors="coerce"))
                except Exception:
                    continue
                name_to_id[str(r["name"]).strip()] = pid

    # 2) team pitchers files
    for p in glob.glob(TEAM_PITCHERS_GLOB):
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if {"name", "player_id"}.issubset(df.columns):
            df = df.dropna(subset=["name", "player_id"])
            for _, r in df.iterrows():
                try:
                    pid = int(pd.to_numeric(r["player_id"], errors="coerce"))
                except Exception:
                    continue
                name_to_id[str(r["name"]).strip()] = pid

    # 3) overrides (authoritative)
    name_to_id.update(OVERRIDES)

    return name_to_id

def resolve_id(pitcher_name: str, existing_id, name_to_id: dict):
    # Leave "Undecided" as unresolved (we'll placeholder later)
    if isinstance(pitcher_name, str) and pitcher_name.strip().lower() == "undecided":
        return pd.NA
    # If already present, keep
    if pd.notna(existing_id):
        return existing_id
    # Lookup by exact name
    pid = name_to_id.get(str(pitcher_name).strip())
    return pid if pid is not None else pd.NA

def assign_placeholders(games: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    For any row where pitcher_home_id / pitcher_away_id is NA, assign a numeric placeholder:
      home:  -(home_team_id)
      away:  -(away_team_id)
    Return updated df and log lines.
    """
    logs: list[str] = []
    # Ensure team ids are numeric
    games["home_team_id"] = _to_int64(games["home_team_id"])
    games["away_team_id"] = _to_int64(games["away_team_id"])

    # Track provenance
    if "pitcher_home_id_src" not in games.columns:
        games["pitcher_home_id_src"] = "resolved"
    if "pitcher_away_id_src" not in games.columns:
        games["pitcher_away_id_src"] = "resolved"

    # Home side
    mask_home_na = games["pitcher_home_id"].isna()
    # Away side
    mask_away_na = games["pitcher_away_id"].isna()

    # Compute placeholders
    # Use negative team ids when available; if team id missing, fallback to a stable negative based on game_id+side.
    # This ensures Int64 dtype and no collision with real MLB IDs (which are positive).
    def _placeholder(row, side: str):
        if side == "home":
            tid = row["home_team_id"]
        else:
            tid = row["away_team_id"]
        if pd.notna(tid):
            return -int(tid)
        gid = row["game_id"] if pd.notna(row["game_id"]) else 0
        # side flag: home=1, away=2 to keep them distinct
        side_flag = 1 if side == "home" else 2
        return -int(gid) - side_flag * 1_000_000

    # Apply placeholders & provenance
    if mask_home_na.any():
        games.loc[mask_home_na, "pitcher_home_id"] = games.loc[mask_home_na].apply(lambda r: _placeholder(r, "home"), axis=1)
        games.loc[mask_home_na, "pitcher_home_id"] = _to_int64(games.loc[mask_home_na, "pitcher_home_id"])
        games.loc[mask_home_na, "pitcher_home_id_src"] = "placeholder"
    if mask_away_na.any():
        games.loc[mask_away_na, "pitcher_away_id"] = games.loc[mask_away_na].apply(lambda r: _placeholder(r, "away"), axis=1)
        games.loc[mask_away_na, "pitcher_away_id"] = _to_int64(games.loc[mask_away_na, "pitcher_away_id"])
        games.loc[mask_away_na, "pitcher_away_id_src"] = "placeholder"

    # Build logs
    for _, r in games[(games["pitcher_home_id_src"] == "placeholder") | (games["pitcher_away_id_src"] == "placeholder")].iterrows():
        logs.append(
            f"game_id={r['game_id']}, "
            f"home={r['home_team']} (id={r['home_team_id']}), away={r['away_team']} (id={r['away_team_id']}), "
            f"pitcher_home='{r['pitcher_home']}' -> {r['pitcher_home_id']} [{r['pitcher_home_id_src']}], "
            f"pitcher_away='{r['pitcher_away']}' -> {r['pitcher_away_id']} [{r['pitcher_away_id_src']}]"
        )
    return games, logs

def main():
    games = load_games()
    name_to_id = build_name_to_id()

    # Resolve IDs first (natural sources)
    games["pitcher_home_id"] = games.apply(
        lambda r: resolve_id(r["pitcher_home"], r.get("pitcher_home_id", pd.NA), name_to_id), axis=1
    )
    games["pitcher_away_id"] = games.apply(
        lambda r: resolve_id(r["pitcher_away"], r.get("pitcher_away_id", pd.NA), name_to_id), axis=1
    )

    # Coerce to Int64 after resolution
    for col in ("pitcher_home_id", "pitcher_away_id"):
        games[col] = _to_int64(games[col])

    # Assign placeholders for any remaining NA
    games, placeholder_logs = assign_placeholders(games)

    # Ensure output folder & write
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)

    # Write summary log of placeholders
    if placeholder_logs:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "w") as f:
            f.write("Pitcher ID placeholders assigned for unresolved starters:\n")
            f.write("\n".join(placeholder_logs) + "\n")

    # Console summary (visible in CI logs)
    total = len(games)
    ph = int((games.get("pitcher_home_id_src") == "placeholder").sum())
    pa = int((games.get("pitcher_away_id_src") == "placeholder").sum())
    print(f"✅ inject_pitcher_ids_into_games: rows={total}, placeholders_home={ph}, placeholders_away={pa}")

if __name__ == "__main__":
    main()
