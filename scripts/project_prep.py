#!/usr/bin/env python3
# scripts/project_prep.py
import pandas as pd
from pathlib import Path

STARTERS_IN      = Path("data/end_chain/final/startingpitchers.csv")
TODAY_GAMES      = Path("data/raw/todaysgames_normalized.csv")
STADIUM_MASTER   = Path("data/manual/stadium_master.csv")

# Outputs
STARTERS_OUT     = Path("data/end_chain/final/startingpitchers.csv")  # overwrite in place
ENRICHED_OUT     = Path("data/raw/startingpitchers_with_opp_context.csv")

def _as_id(s: pd.Series) -> pd.Series:
    """Clean ids to plain strings, strip whitespace and trailing .0"""
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    out = out.where(~out.isin({"", "nan", "None"}), pd.NA)
    return out

def _read_csv(p: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(p)
    except FileNotFoundError:
        print(f"Load warning: missing {p}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Load warning: failed reading {p}: {e}")
        return pd.DataFrame()

def project_prep():
    # ---------- Load ----------
    starters = _read_csv(STARTERS_IN)
    games    = _read_csv(TODAY_GAMES)
    stadiums = _read_csv(STADIUM_MASTER)

    if starters.empty:
        print(f"Load error: starters file empty/missing at {STARTERS_IN}")
        return
    if games.empty:
        print(f"Load error: todays games empty/missing at {TODAY_GAMES}")
        return

    # ---------- Normalize keys ----------
    # starters: player_id, game_id may exist (keep, but clean)
    for c in ("player_id", "game_id", "team_id"):
        if c in starters.columns:
            starters[c] = _as_id(starters[c])

    # games: ids & pitcher ids
    for c in ("game_id","home_team_id","away_team_id","pitcher_home_id","pitcher_away_id"):
        if c in games.columns:
            games[c] = _as_id(games[c])

    # Stadiums: team_id is the home team id key we’ll join on
    if not stadiums.empty and "team_id" in stadiums.columns:
        stadiums["team_id"] = _as_id(stadiums["team_id"])

    # ---------- Build today’s pitcher → game map ----------
    # Long mapping of pitcher_id to (game_id, role, home_team_id, park_factor)
    have_pf = "park_factor" in games.columns
    long_rows = []
    for _, r in games.iterrows():
        gid = r.get("game_id", pd.NA)
        pf  = r.get("park_factor", pd.NA) if have_pf else pd.NA
        ht  = r.get("home_team_id", pd.NA)
        at  = r.get("away_team_id", pd.NA)

        # Home pitcher
        ph_id = r.get("pitcher_home_id", pd.NA)
        if pd.notna(ph_id):
            long_rows.append({"player_id": ph_id, "game_id_tg": gid, "role": "HOME", "home_team_id": ht, "park_factor": pf})
        else:
            # undecided: create placeholder so downstream can still join by game
            long_rows.append({"player_id": f"UND_{gid}_HOME", "game_id_tg": gid, "role": "HOME", "home_team_id": ht, "park_factor": pf})

        # Away pitcher
        pa_id = r.get("pitcher_away_id", pd.NA)
        if pd.notna(pa_id):
            long_rows.append({"player_id": pa_id, "game_id_tg": gid, "role": "AWAY", "home_team_id": ht, "park_factor": pf})
        else:
            long_rows.append({"player_id": f"UND_{gid}_AWAY", "game_id_tg": gid, "role": "AWAY", "home_team_id": ht, "park_factor": pf})

    tg_long = pd.DataFrame(long_rows)
    # ensure clean ids
    tg_long["player_id"] = _as_id(tg_long["player_id"])
    tg_long["game_id_tg"] = _as_id(tg_long["game_id_tg"])
    tg_long["home_team_id"] = _as_id(tg_long["home_team_id"])

    # ---------- Attach game_id & park_factor to starters ----------
    # Some starters already have game_id; we coalesce: prefer existing starter game_id, else from today’s games
    # Use left join by player_id (both coerced to str ids)
    merged = starters.merge(tg_long, on="player_id", how="left", suffixes=("", "_tgmap"))

    # Coalesce/repair game_id
    if "game_id" not in merged.columns:
        merged["game_id"] = pd.NA
    merged["game_id"] = merged["game_id"].where(merged["game_id"].notna(), merged["game_id_tg"])

    # If still missing, and we have a last-resort: create stable unknown per-row
    missing_gid = merged["game_id"].isna()
    if missing_gid.any():
        merged.loc[missing_gid, "game_id"] = merged.loc[missing_gid].index.map(lambda i: f"UNKNOWN_{i}")

    # Park factor
    if "park_factor" not in merged.columns:
        merged["park_factor"] = pd.NA
    merged["park_factor"] = merged["park_factor"].where(merged["park_factor"].notna(), merged.get("park_factor_tgmap", pd.NA))

    # ---------- Attach venue/weather context by HOME team (from stadium_master) ----------
    if not stadiums.empty and {"team_id","city","state","timezone","is_dome"}.issubset(stadiums.columns):
        # bring through the home_team_id we already carried on tg_long
        if "home_team_id" not in merged.columns:
            merged["home_team_id"] = merged.get("home_team_id_tgmap", pd.NA)
        venue_cols = ["team_id","city","state","timezone","is_dome"]
        merged = merged.merge(stadiums[venue_cols].drop_duplicates("team_id"),
                              left_on="home_team_id", right_on="team_id", how="left", suffixes=("", "_stadium"))
        # clean up the helper team_id from stadiums
        if "team_id_stadium" in merged.columns:
            merged.drop(columns=["team_id_stadium"], inplace=True, errors="ignore")

    # ---------- Tidy columns ----------
    drop_helpers = [c for c in merged.columns if c.endswith("_tgmap") or c.endswith("_stadium") or c in ("game_id_tg",)]
    merged.drop(columns=drop_helpers, inplace=True, errors="ignore")

    # Make sure core identifiers are strings
    for c in ("player_id","game_id","home_team_id"):
        if c in merged.columns:
            merged[c] = _as_id(merged[c])

    # ---------- Save outputs ----------
    STARTERS_OUT.parent.mkdir(parents=True, exist_ok=True)
    ENRICHED_OUT.parent.mkdir(parents=True, exist_ok=True)

    # Overwrite starters (now with repaired game_id / park_factor / venue context columns if present)
    merged.to_csv(STARTERS_OUT, index=False)

    # Also publish the enriched copy for downstream scripts that prefer this location/name
    merged.to_csv(ENRICHED_OUT, index=False)

    print(f"✅ project_prep: wrote {STARTERS_OUT} and {ENRICHED_OUT} (rows={len(merged)})")

if __name__ == "__main__":
    project_prep()
