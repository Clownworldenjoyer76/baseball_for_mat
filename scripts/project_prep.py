#!/usr/bin/env python3
# scripts/project_prep.py

from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

# ---------- Inputs ----------
PATH_TODAY = Path("data/raw/todaysgames_normalized.csv")  # required
PATH_PITCHER_CLEAN = Path("data/cleaned/pitchers_normalized_cleaned.csv")  # optional (for real skills)
PATH_WEATHER = Path("data/weather_input.csv")  # optional (per-game weather/roof/coords)
PATH_STADIUM = Path("data/manual/stadium_master.csv")  # optional (home venue metadata)

# ---------- Outputs ----------
OUT_DIR = Path("data/end_chain/final")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_STARTERS = OUT_DIR / "startingpitchers.csv"
OUT_STARTERS_COMPAT = OUT_DIR / "startingpitchers_final.csv"  # keep for older scripts

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"NOTE: {path} not found.")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.map(str).str.strip()
        return df
    except Exception as e:
        print(f"WARN: failed to read {path}: {e}")
        return pd.DataFrame()

def _median_safe(s: pd.Series, fallback: float) -> float:
    try:
        v = pd.to_numeric(s, errors="coerce")
        m = float(v.median())
        if np.isnan(m):
            return fallback
        return m
    except Exception:
        return fallback

def _build_starers_long(g: pd.DataFrame) -> pd.DataFrame:
    # Expect at minimum: game_id, home_team_id, away_team_id, pitcher_home_id, pitcher_away_id, pitcher_home, pitcher_away, park_factor
    need = ["game_id","home_team_id","away_team_id","pitcher_home_id","pitcher_away_id","pitcher_home","pitcher_away","park_factor"]
    missing = [c for c in need if c not in g.columns]
    if missing:
        raise SystemExit(f"❌ todaysgames_normalized missing columns: {missing}")

    home = g.rename(columns={
        "home_team_id": "team_id",
        "away_team_id": "opponent_team_id",
        "pitcher_home_id": "player_id",
        "pitcher_home": "name"
    }).assign(is_home=True)

    away = g.rename(columns={
        "away_team_id": "team_id",
        "home_team_id": "opponent_team_id",
        "pitcher_away_id": "player_id",
        "pitcher_away": "name"
    }).assign(is_home=False)

    keep = ["game_id","team_id","opponent_team_id","player_id","name","park_factor","is_home"]
    long = pd.concat([home[keep], away[keep]], ignore_index=True)

    # Normalize IDs to strings; detect undecided
    long["player_id"] = long["player_id"].astype(str)
    long["undecided"] = long["player_id"].isna() | (long["player_id"].str.strip().eq("")) | (long["player_id"].str.lower().isin(["nan", "none", "null"]))
    # Create placeholders where undecided
    mask_u = long["undecided"]
    long.loc[mask_u, "player_id"] = long.loc[mask_u].apply(
        lambda r: f"UNDECIDED_{int(r['game_id'])}_{'H' if r['is_home'] else 'A'}", axis=1
    )
    # Basic dtypes
    long["team_id"] = pd.to_numeric(long["team_id"], errors="coerce").astype("Int64")
    long["opponent_team_id"] = pd.to_numeric(long["opponent_team_id"], errors="coerce").astype("Int64")
    long["park_factor"] = pd.to_numeric(long["park_factor"], errors="coerce")
    return long

def _attach_weather(long: pd.DataFrame, w: pd.DataFrame) -> pd.DataFrame:
    if w.empty:
        return long
    # Expect: game_id, venue, city, latitude, longitude, roof_type (per your provided schema)
    cols = [c for c in ["game_id","venue","city","latitude","longitude","roof_type"] if c in w.columns]
    if "game_id" not in cols:
        return long
    add = w[cols].drop_duplicates("game_id")
    return long.merge(add, on="game_id", how="left")

def _attach_venue(long: pd.DataFrame, v: pd.DataFrame) -> pd.DataFrame:
    if v.empty:
        return long
    # Expect from stadium_master: team_id (home), city, state, timezone, is_dome
    cols = [c for c in ["team_id","city","state","timezone","is_dome"] if c in v.columns]
    if "team_id" not in cols:
        return long
    add = v[cols].drop_duplicates("team_id").rename(columns={
        "city": "home_city",
        "state": "home_state",
        "timezone": "home_timezone",
        "is_dome": "home_is_dome"
    })
    return long.merge(add, on="team_id", how="left")

def _attach_skills(long: pd.DataFrame, pc: pd.DataFrame) -> pd.DataFrame:
    # Compute league medians for fallbacks
    if pc.empty:
        k_per_ip_med = 1.0
        bb_per_ip_med = 0.35
    else:
        pc_cols = pc.columns.map(str)
        ip_col = "p_formatted_ip" if "p_formatted_ip" in pc_cols else None
        k_col = "strikeout" if "strikeout" in pc_cols else None
        bb_col = "walk" if "walk" in pc_cols else None

        ip = pd.to_numeric(pc.get(ip_col, pd.Series(dtype=float)).apply(_ip_to_float), errors="coerce")
        k_tot = pd.to_numeric(pc.get(k_col, pd.Series(dtype=float)), errors="coerce")
        bb_tot = pd.to_numeric(pc.get(bb_col, pd.Series(dtype=float)), errors="coerce")
        k_per_ip = (k_tot / ip).replace([np.inf, -np.inf], np.nan)
        bb_per_ip = (bb_tot / ip).replace([np.inf, -np.inf], np.nan)

        k_per_ip_med = _median_safe(k_per_ip, 1.0)
        bb_per_ip_med = _median_safe(bb_per_ip, 0.35)

    # Join available real skills by player_id when present
    join_cols = []
    if not pc.empty and "player_id" in pc.columns:
        join_cols = ["player_id"]
        use_cols = ["player_id"]
        for c in ["p_formatted_ip","strikeout","walk","p_game","k_percent","bb_percent"]:
            if c in pc.columns:
                use_cols.append(c)
        merged = long.merge(pc[use_cols].drop_duplicates("player_id"), on="player_id", how="left", suffixes=("", "_pc"))
    else:
        merged = long.copy()

    # Derive projection-facing simple fields
    # innings_pitched: use recent per-appearance if possible, else 5.0
    ip_float = merged.get("p_formatted_ip", pd.Series(index=merged.index)).apply(_ip_to_float)
    apps = pd.to_numeric(merged.get("p_game", pd.Series(index=merged.index)), errors="coerce")
    ip_per_app = (pd.to_numeric(ip_float, errors="coerce") / apps).replace([np.inf, -np.inf], np.nan)
    ip_proj = ip_per_app.clip(lower=0.2, upper=7.2)
    merged["innings_pitched"] = ip_proj.fillna(5.0)

    # K/BB projections from per-IP rates (real when available, else medians)
    k_tot = pd.to_numeric(merged.get("strikeout", pd.Series(index=merged.index)), errors="coerce")
    bb_tot = pd.to_numeric(merged.get("walk", pd.Series(index=merged.index)), errors="coerce")
    ip_season = pd.to_numeric(ip_float, errors="coerce")

    k_per_ip_real = (k_tot / ip_season).replace([np.inf, -np.inf], np.nan)
    bb_per_ip_real = (bb_tot / ip_season).replace([np.inf, -np.inf], np.nan)

    merged["K_per_ip_used"] = k_per_ip_real.fillna(k_per_ip_med)
    merged["BB_per_ip_used"] = bb_per_ip_real.fillna(bb_per_ip_med)

    merged["strikeouts"] = (merged["innings_pitched"] * merged["K_per_ip_used"]).clip(lower=0.0, upper=15.0)
    merged["walks"]      = (merged["innings_pitched"] * merged["BB_per_ip_used"]).clip(lower=0.0, upper=8.0)

    # Keep simple rate placeholders (not critical but useful)
    merged["k_percent"]  = pd.to_numeric(merged.get("k_percent", pd.NA), errors="coerce").fillna(0.23)
    merged["bb_percent"] = pd.to_numeric(merged.get("bb_percent", pd.NA), errors="coerce").fillna(0.08)

    return merged

def _ip_to_float(x) -> float:
    """Convert baseball IP like 47.1/47.2 to 47.333/47.667."""
    try:
        s = str(x)
        if s.strip() == "" or s.lower() == "nan":
            return np.nan
        if "." not in s:
            return float(s)
        whole, frac = s.split(".", 1)
        whole = int(whole)
        frac = ''.join(ch for ch in frac if ch.isdigit())
        frac_i = int(frac) if frac else 0
        if frac_i == 0:
            add = 0.0
        elif frac_i == 1:
            add = 1/3
        elif frac_i == 2:
            add = 2/3
        else:
            return float(x)
        return whole + add
    except Exception:
        try:
            return float(x)
        except Exception:
            return np.nan

def project_prep():
    # 1) Load inputs
    g  = _read_csv(PATH_TODAY)
    pc = _read_csv(PATH_PITCHER_CLEAN)
    w  = _read_csv(PATH_WEATHER)
    st = _read_csv(PATH_STADIUM)

    if g.empty:
        raise SystemExit("❌ No games found in data/raw/todaysgames_normalized.csv")

    # 2) Build two-row starters frame (home/away)
    starters = _build_starers_long(g)

    # 3) Attach skills (with fallbacks + undecided placeholders supported)
    starters = _attach_skills(starters, pc)

    # 4) Attach weather (by game_id) and venue (by home team_id)
    starters = _attach_weather(starters, w)
    starters = _attach_venue(starters, st)

    # 5) Basic housekeeping / column order
    base_cols = [
        "player_id","game_id","team_id","opponent_team_id","is_home","name",
        "innings_pitched","strikeouts","walks","k_percent","bb_percent","park_factor",
        # Weather (if available)
        "venue","city","latitude","longitude","roof_type",
        # Venue meta (home team)
        "home_city","home_state","home_timezone","home_is_dome",
    ]
    # Keep whatever exists among base_cols, plus any extras already present
    cols = [c for c in base_cols if c in starters.columns]
    extras = [c for c in starters.columns if c not in cols]
    out = starters[cols + extras].copy()

    # 6) Save outputs (both filenames for compatibility)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_STARTERS, index=False)
    out.to_csv(OUT_STARTERS_COMPAT, index=False)
    print(f"✅ Wrote starters → {OUT_STARTERS}  and  {OUT_STARTERS_COMPAT}  (rows={len(out)})")

if __name__ == "__main__":
    project_prep()
