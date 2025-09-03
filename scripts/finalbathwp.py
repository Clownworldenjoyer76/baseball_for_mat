# scripts/finalbathwp.py
import os
import subprocess
import pandas as pd

# ------------ Config ------------
BATTERS_PATH = "data/end_chain/first/raw/bat_hwp_dirty.csv"
GAMES_PATH   = "data/end_chain/cleaned/games_today_cleaned.csv"

PF_DAY_PATH        = "data/manual/park_factors_day.csv"
PF_NIGHT_PATH      = "data/manual/park_factors_night.csv"
PF_ROOF_CLOSED_PATH= "data/manual/park_factors_roof_closed.csv"

OUT_DIR  = "data/end_chain/final"
OUT_FILE = os.path.join(OUT_DIR, "finalbathwp.csv")

# Required keys
REQ_BAT_COLS  = {"player_id", "game_id"}
REQ_GAME_COLS = {"game_id"}  # we’ll append optional game fields if present

# Optional game fields we append when present
KEEP_GAME_COLS = [
    "home_team", "away_team", "game_time", "game_time_et",
    "pitcher_home", "pitcher_away",
    "home_team_id","away_team_id",
    "pitcher_home_id","pitcher_away_id",
    "venue","city","latitude","longitude",
    "roof_type","notes",
    "home_team_id_wx","away_team_id_wx",
    "home_team_wx","away_team_wx",
    "matched_forecast_day","matched_forecast_time",
    "temperature","wind_speed","wind_direction",
    "humidity","precipitation","condition",
    "fetched_at","date"
]

# Columns explicitly dropped if present
DROP_IRRELEVANT = {"last_name","first_name","year","player_age"}

# ------------ Helpers ------------
def load_park_factors():
    def read_pf(path, src_name):
        if not os.path.exists(path):
            return pd.DataFrame(columns=["venue","park_factor_100"]).assign(park_factor_src=src_name)
        df = pd.read_csv(path)
        # Expect columns like: venue, park_factor (or similar). Normalize to 100-based.
        # Accept either a 1.00-based or 100-based input. If mean < 5 assume 1.00 scale.
        pf_col = None
        for c in df.columns:
            if c.lower() in {"park_factor","park_factor_100","pf","parkfactor"}:
                pf_col = c
                break
        if pf_col is None:
            # Nothing we can do—return empty to avoid crashes
            return pd.DataFrame(columns=["venue","park_factor_100"]).assign(park_factor_src=src_name)

        out = df.rename(columns={pf_col:"_pf", "Venue":"venue", "venue_name":"venue"})
        if "venue" not in out.columns:
            # try to construct venue from stadium/ballpark fields if they exist
            for alt in ["stadium","ballpark","park","name"]:
                if alt in out.columns:
                    out = out.rename(columns={alt:"venue"})
                    break
        if "venue" not in out.columns:
            out["venue"] = pd.NA

        # scale detection
        scale = 100 if out["_pf"].dropna().mean() >= 5 else 100
        out["park_factor_100"] = out["_pf"] * (100 if scale == 100 else 100)  # force 100-based
        out = out[["venue","park_factor_100"]].copy()
        out["park_factor_src"] = src_name
        return out

    day   = read_pf(PF_DAY_PATH,   "day")
    night = read_pf(PF_NIGHT_PATH, "night")
    roofc = read_pf(PF_ROOF_CLOSED_PATH, "roof_closed")
    return day, night, roofc

def choose_pf_key(g):
    """Return which PF table to use for a given game row: roof_closed > (day/night by local game_time).
       If roof_type contains 'closed', use roof_closed.
    """
    roof = str(g.get("roof_type", "") or "").lower()
    if "closed" in roof:
        return "roof_closed"
    # If we have a local clock like '7:10 PM', treat 6:00 PM or later as night
    t = str(g.get("game_time","") or "")
    if any(p in t for p in ["PM","pm"]):
        try:
            hh = int(t.split(":")[0])
            if "PM" in t.upper() and hh >= 6:
                return "night"
        except Exception:
            pass
    return "day"

def add_park_factors(df):
    # Merge venue & roof info from games, then map PF
    day, night, roofc = load_park_factors()

    # We’ll attach the correct PF row-by-row
    df["__pf_key"] = df.apply(choose_pf_key, axis=1)

    # Build a single mapping DF by stacking with a key
    day["__pf_key"]   = "day"
    night["__pf_key"] = "night"
    roofc["__pf_key"] = "roof_closed"
    pf_all = pd.concat([day, night, roofc], ignore_index=True)

    # Merge on (venue, __pf_key)
    # If venue is missing, we won’t find a match (will become NaN)
    out = df.merge(
        pf_all.rename(columns={"venue":"__venue_pf"}),
        left_on=["venue","__pf_key"],
        right_on=["__venue_pf","__pf_key"],
        how="left",
        suffixes=("","")
    )
    # If still missing, mark unknown
    out["park_factor_100"] = out["park_factor_100"].round(3)
    out["park_factor_src"] = out["park_factor_src"].fillna("unknown")
    out = out.drop(columns=["__venue_pf","__pf_key"])
    return out

def safe_git_commit(path, message):
    try:
        subprocess.run(["git", "add", path], check=True)
        # commit only if there are staged changes
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode != 0:  # there ARE staged changes
            subprocess.run(["git", "commit", "-m", message], check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ Committed and pushed", os.path.basename(path))
        else:
            print("ℹ️ No content change in", os.path.basename(path), "- skipping commit.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operation skipped/failed: {e}")

# ------------ Main ------------
def main():
    if not os.path.exists(BATTERS_PATH):
        raise SystemExit(f"❌ Missing batters file: {BATTERS_PATH}")
    if not os.path.exists(GAMES_PATH):
        raise SystemExit(f"❌ Missing games file: {GAMES_PATH}")

    bat   = pd.read_csv(BATTERS_PATH)
    games = pd.read_csv(GAMES_PATH)

    # Required columns
    missing_b = REQ_BAT_COLS - set(bat.columns)
    missing_g = REQ_GAME_COLS - set(games.columns)
    if missing_b:
        raise SystemExit(f"❌ {BATTERS_PATH} missing required columns: {sorted(missing_b)}")
    if missing_g:
        raise SystemExit(f"❌ {GAMES_PATH} missing required columns: {sorted(missing_g)}")

    # Drop explicitly irrelevant columns if present
    bat = bat.drop(columns=[c for c in DROP_IRRELEVANT if c in bat.columns], errors="ignore")

    # Normalize merge keys
    bat["game_id"]   = bat["game_id"].astype("string")
    games["game_id"] = games["game_id"].astype("string")

    # Keep full batting schema; only append game fields
    game_cols = ["game_id"] + [c for c in KEEP_GAME_COLS if c in games.columns]
    games_small = games[game_cols].drop_duplicates()

    merged = bat.merge(games_small, on="game_id", how="left")

    # Add park factors (100-based) and source; leave unknowns as 'unknown'
    if "venue" in merged.columns:
        merged = add_park_factors(merged)
    else:
        merged["park_factor_100"] = pd.NA
        merged["park_factor_src"] = "unknown"

    # Write
    os.makedirs(OUT_DIR, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    print(f"📝 Wrote {OUT_FILE} (rows={len(merged)}, cols={len(merged.columns)})")

    # Commit only if changed
    safe_git_commit(
        OUT_FILE,
        "finalbathwp: game_id merge + 100-based park factors (day/night/roof_closed)"
    )

if __name__ == "__main__":
    main()
