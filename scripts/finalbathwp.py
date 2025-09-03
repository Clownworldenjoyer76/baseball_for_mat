# scripts/finalbathwp.py
#
# Build finalbathwp.csv for HOME batters by merging dirty HWP with today's games,
# attaching 100-based park factors (day/night/roof_closed) and preserving both
# game_time and game_time_et when present.
#
# Directives applied:
# 1) Drop irrelevant columns (last_name, first_name, year, player_age)
# 2) Leave roof/notes redundancy alone
# 3) Keep both game_time and game_time_et if present
# 4) Pitcher gaps: leave as-is
# 5) Park factor scaling: force 100-based; detect source (day/night/roof_closed),
#    fall back to "unknown" if no mapping
# 6) Keep adjusted columns distinct (we only pass them through; no renaming)
# 7) Bloat is fine

import os
import re
import subprocess
import pandas as pd
import numpy as np

# Inputs
BATTERS_PATH = "data/end_chain/first/raw/bat_hwp_dirty.csv"          # HOME batters dirty
GAMES_PATH   = "data/end_chain/cleaned/games_today_cleaned.csv"
PF_DAY_PATH  = "data/manual/park_factors_day.csv"
PF_NGT_PATH  = "data/manual/park_factors_night.csv"
PF_ROOF_PATH = "data/manual/park_factors_roof_closed.csv"

# Output
OUT_DIR  = "data/end_chain/final"
OUT_FILE = os.path.join(OUT_DIR, "finalbathwp.csv")

# Required minimal columns
REQ_BAT_COLS  = {"player_id", "game_id"}
REQ_GAME_COLS = {"game_id"}  # everything else is optional

# Irrelevant columns to drop if present
DROP_BAT_COLS = {"last_name", "first_name", "year", "player_age"}

# Helper: parse "7:35 PM" -> hour (0-23); returns np.nan on failure
_TIME_RE = re.compile(r"^\s*(\d{1,2})\s*:\s*(\d{2})\s*([AP]M)\s*$", re.IGNORECASE)
def parse_ampm_hour(val):
    if not isinstance(val, str):
        return np.nan
    m = _TIME_RE.match(val.strip())
    if not m:
        return np.nan
    hh = int(m.group(1))
    mm = int(m.group(2))
    ampm = m.group(3).upper()
    if hh == 12:
        hh = 0
    hour24 = hh + (12 if ampm == "PM" else 0)
    return hour24

def is_night_row(game_time_str, game_time_et_str):
    """Night if any available local/ET time parses to hour >= 18, else day."""
    h_local = parse_ampm_hour(game_time_str)
    h_et    = parse_ampm_hour(game_time_et_str)
    cand = [h for h in [h_local, h_et] if not (isinstance(h, float) and np.isnan(h))]
    if not cand:
        return False
    return any(h >= 18 for h in cand)

def load_pf_map(path):
    """
    Load a park factor file and return:
      - a dict of {key_value: factor}
      - the name of the key column used
    We try keys in priority: 'venue', 'home_team_id', 'home_team'.
    We try factor column among: 'park_factor','pf','factor','value'.
    """
    if not os.path.exists(path):
        return {}, None
    df = pd.read_csv(path)

    # find key col
    key_cols_try = [c for c in ["venue", "home_team_id", "home_team"] if c in df.columns]
    if not key_cols_try:
        return {}, None
    key_col = key_cols_try[0]

    # find factor col
    fac_cols_try = [c for c in ["park_factor", "pf", "factor", "value"] if c in df.columns]
    if not fac_cols_try:
        # if there's only two columns, assume the non-key is factor
        if df.shape[1] == 2:
            fac_cols_try = [col for col in df.columns if col != key_col]
        if not fac_cols_try:
            return {}, None
    fac_col = fac_cols_try[0]

    # Clean factor type
    out = {}
    for _, r in df[[key_col, fac_col]].dropna().iterrows():
        k = r[key_col]
        v = r[fac_col]
        try:
            v = float(v)
        except Exception:
            continue
        out[str(k)] = v
    return out, key_col

def choose_pf_keycolumn(games_df):
    """Pick which games column we will use to look up PF: venue > home_team_id > home_team."""
    for c in ["venue", "home_team_id", "home_team"]:
        if c in games_df.columns:
            return c
    return None

def map_pf(series_keys, pf_map):
    """Series.map via dictionary with safe fallback to NaN."""
    if not pf_map:
        return pd.Series(np.nan, index=series_keys.index)
    return series_keys.astype("string").map(lambda x: pf_map.get(x, np.nan))

def scale_to_100(x):
    """Force 100-based scaling. If < 3, assume it's 1.19 style and *100; else return as-is."""
    if pd.isna(x):
        return np.nan
    try:
        f = float(x)
    except Exception:
        return np.nan
    return f * 100.0 if f < 3.0 else f

def main():
    # --- Load inputs ---
    if not os.path.exists(BATTERS_PATH):
        raise SystemExit(f"❌ Missing batters file: {BATTERS_PATH}")
    if not os.path.exists(GAMES_PATH):
        raise SystemExit(f"❌ Missing games file: {GAMES_PATH}")

    bat = pd.read_csv(BATTERS_PATH)
    games = pd.read_csv(GAMES_PATH)

    # --- Validate required columns ---
    missing_bat  = REQ_BAT_COLS - set(bat.columns)
    missing_game = REQ_GAME_COLS - set(games.columns)
    if missing_bat:
        raise SystemExit(f"❌ {BATTERS_PATH} missing required columns: {sorted(missing_bat)}")
    if missing_game:
        raise SystemExit(f"❌ {GAMES_PATH} missing required columns: {sorted(missing_game)}")

    # --- Normalize dtypes for merge keys ---
    bat["game_id"]   = bat["game_id"].astype("string")
    games["game_id"] = games["game_id"].astype("string")
    if "player_id" in bat.columns:
        bat["player_id"] = bat["player_id"].astype("string")

    # --- Drop irrelevant columns in bat (if present) ---
    drop_cols = [c for c in DROP_BAT_COLS if c in bat.columns]
    if drop_cols:
        bat = bat.drop(columns=drop_cols)

    # --- Merge ALL game columns (bloat OK), preserving both game_time columns if present ---
    # We keep batter columns' names; suffix games with "_g" if there are collisions (rare and harmless).
    merged = bat.merge(games, on="game_id", how="left", suffixes=("", "_g"))

    # --- Determine day/night/roof flags from games data ---
    # roof_closed if roof_type contains "closed" or "dome"
    roof_col = "roof_type" if "roof_type" in merged.columns else None
    roof_closed = (
        merged[roof_col].astype(str).str.lower().str.contains("closed|dome")
        if roof_col else pd.Series(False, index=merged.index)
    )

    # Night detection from either game_time or game_time_et, prefer actual parsed times
    gt  = merged["game_time"]    if "game_time"    in merged.columns else pd.Series([np.nan]*len(merged))
    gte = merged["game_time_et"] if "game_time_et" in merged.columns else pd.Series([np.nan]*len(merged))
    night_flag = [is_night_row(a, b) for a, b in zip(gt, gte)]
    night_flag = pd.Series(night_flag, index=merged.index)

    # If roof is closed, override to roof source regardless of time of day
    src_series = pd.Series(np.where(roof_closed, "roof_closed", np.where(night_flag, "night", "day")),
                           index=merged.index, dtype="string")

    # --- Load park factor manuals & prepare mappings ---
    pf_day_map,  pf_day_key  = load_pf_map(PF_DAY_PATH)
    pf_ngt_map,  pf_ngt_key  = load_pf_map(PF_NGT_PATH)
    pf_roof_map, pf_roof_key = load_pf_map(PF_ROOF_PATH)

    # Decide which games column to use for lookup
    pf_key_col = choose_pf_keycolumn(merged)
    if pf_key_col is None:
        # No viable key in games to map PF; everything becomes unknown
        pf_day_vals  = pd.Series(np.nan, index=merged.index)
        pf_ngt_vals  = pd.Series(np.nan, index=merged.index)
        pf_roof_vals = pd.Series(np.nan, index=merged.index)
    else:
        key_series = merged[pf_key_col].astype("string")
        # Prefer exact mapping where the file used the same key column; otherwise still try (best-effort)
        pf_day_vals  = map_pf(key_series,  pf_day_map)
        pf_ngt_vals  = map_pf(key_series,  pf_ngt_map)
        pf_roof_vals = map_pf(key_series,  pf_roof_map)

    # --- Choose park factor by source, then scale to 100-basis ---
    pf_chosen = (
        np.where(src_series.eq("roof_closed"), pf_roof_vals,
                 np.where(src_series.eq("night"), pf_ngt_vals, pf_day_vals))
    )
    pf_chosen = pd.to_numeric(pf_chosen, errors="coerce")

    park_factor_raw  = pd.Series(pf_chosen, index=merged.index)  # raw number from manuals
    park_factor_100  = park_factor_raw.map(scale_to_100)
    park_factor_src  = np.where(~park_factor_raw.isna(), src_series, "unknown")
    park_factor_src  = pd.Series(park_factor_src, index=merged.index, dtype="string")

    # --- Attach PF columns ---
    merged["park_factor_raw"]  = park_factor_raw
    merged["park_factor_100"]  = park_factor_100
    merged["park_factor_src"]  = park_factor_src

    # (We intentionally keep roof/notes/etc as-is per directives.)

    # --- Write output ---
    os.makedirs(OUT_DIR, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    print(f"✅ Built {OUT_FILE} (rows={len(merged)}, cols={len(merged.columns)})")

    # --- Commit ---
    try:
        subprocess.run(["git", "add", OUT_FILE], check=True)
        subprocess.run(["git", "commit", "-m",
                        "finalbathwp: merge HWP with games; add 100-based park factors (day/night/roof_closed)"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed finalbathwp.csv")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push skipped or failed: {e}")

if __name__ == "__main__":
    main()
