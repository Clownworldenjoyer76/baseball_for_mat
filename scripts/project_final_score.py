#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

# -------- Config --------
TARGET_AVG_TOTAL = 9.0
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
GAMES_CANDIDATES = [
    Path("data/cleaned/games_today_cleaned.csv"),
    Path("data/end_chain/cleaned/games_today_cleaned.csv"),
    Path("data/raw/todaysgames_normalized.csv"),
]
WEATHER_FILE = Path("data/weather_adjustments.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

# -------- Helpers --------
def _norm_team(x): return str(x).strip().lower()
def _norm_name(x): return str(x).strip().lower()

def _require_file(p: Path, label: str):
    if not p.exists():
        raise SystemExit(f"❌ Required file missing: {label} → {p}")

def _pick_first_existing(paths):
    for p in paths:
        if p.exists():
            return p
    return None

def _pick_col(df, options):
    cols = set(df.columns)
    for c in options:
        if c in cols:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in options:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

# -------- Load strengths --------
def load_strengths():
    _require_file(BATTER_FILE, "batter props")
    bat = pd.read_csv(BATTER_FILE)
    if not {"team","prop_type","projection"}.issubset(bat.columns):
        raise SystemExit("❌ batter file missing columns: team, prop_type, projection")
    bat["team"] = bat["team"].map(_norm_team)
    bats_tb = bat[bat["prop_type"] == "total_bases"].copy()
    # Top-9 average per team
    bat_strength = (
        bats_tb.groupby("team")["projection"]
        .apply(lambda s: float(np.mean(sorted(s, reverse=True)[:9])) if len(s) else 0.0)
        .to_dict()
    )

    _require_file(PITCHER_FILE, "pitcher props")
    pit = pd.read_csv(PITCHER_FILE)
    if not {"name","mega_z"}.issubset(pit.columns):
        raise SystemExit("❌ pitcher file missing columns: name, mega_z")
    pit["name"] = pit["name"].map(_norm_name)
    pitch_strength = pit.dropna(subset=["name"]).set_index("name")["mega_z"].to_dict()

    print(f"📊 Loaded strengths → bat_teams={len(bat_strength)} pitcher_names={len(pitch_strength)}")
    return bat_strength, pitch_strength

# -------- Main --------
def main():
    bat_strength, pitch_strength = load_strengths()

    games_path = _pick_first_existing(GAMES_CANDIDATES)
    if games_path is None:
        raise SystemExit("❌ No games file found. Looked for: " + ", ".join(str(p) for p in GAMES_CANDIDATES))
    games = pd.read_csv(games_path)
    print(f"🧾 Using games file: {games_path} (rows={len(games)})")

    # Resolve required columns with flexible names
    h_col = _pick_col(games, ["home_team","home"])
    a_col = _pick_col(games, ["away_team","away"])
    hp_col = _pick_col(games, ["home_pitcher","pitcher_home","home_sp","home_starter"])
    ap_col = _pick_col(games, ["away_pitcher","pitcher_away","away_sp","away_starter"])
    date_col = _pick_col(games, ["date","game_date"])
    if not (h_col and a_col):
        raise SystemExit("❌ games file missing home_team/away_team columns")

    # Normalize
    games["home_team_norm"] = games[h_col].map(_norm_team)
    games["away_team_norm"] = games[a_col].map(_norm_team)
    if hp_col: games["home_pitcher_norm"] = games[hp_col].map(_norm_name)
    if ap_col: games["away_pitcher_norm"] = games[ap_col].map(_norm_name)

    # Date
    if date_col and games[date_col].notna().any():
        games["date"] = games[date_col].fillna(method="ffill").fillna(method="bfill").astype(str)
    else:
        games["date"] = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Weather
    wf = 1.0
    if WEATHER_FILE.exists():
        wx = pd.read_csv(WEATHER_FILE)
        if {"home_team","weather_factor"}.issubset(wx.columns):
            wx = wx.copy()
            wx["home_team_norm"] = wx["home_team"].map(_norm_team)
            wx["weather_factor"] = pd.to_numeric(wx["weather_factor"], errors="coerce")
            games = games.merge(wx[["home_team_norm","weather_factor"]], left_on="home_team_norm", right_on="home_team_norm", how="left")
        else:
            games["weather_factor"] = 1.0
    else:
        games["weather_factor"] = 1.0
    games["weather_factor"] = games["weather_factor"].fillna(1.0)

    # Scores
    hs_list, as_list = [], []
    miss_hp = miss_ap = 0
    for _, r in games.iterrows():
        bh = bat_strength.get(r["home_team_norm"], 0.0)
        ba = bat_strength.get(r["away_team_norm"], 0.0)

        ph = 0.0
        pa = 0.0
        if hp_col:
            ph = pitch_strength.get(r.get("home_pitcher_norm",""), 0.0)
            if ph == 0.0: miss_hp += 1
        if ap_col:
            pa = pitch_strength.get(r.get("away_pitcher_norm",""), 0.0)
            if pa == 0.0: miss_ap += 1

        base_home = 4.5 + (bh - pa)
        base_away = 4.5 + (ba - ph)

        w = float(r["weather_factor"]) if pd.notna(r["weather_factor"]) else 1.0
        adj_home = base_home * (w ** 0.5)
        adj_away = base_away * (w ** 0.5)

        hs_list.append(adj_home)
        as_list.append(adj_away)

    games["home_score"] = hs_list
    games["away_score"] = as_list
    avg_total = (games["home_score"] + games["away_score"]).mean()

    # Scale totals gently toward target only if slate big & out of band
    if len(games) >= 6 and (avg_total < 8.2 or avg_total > 9.8):
        scale = TARGET_AVG_TOTAL / avg_total
        games["home_score"] *= scale
        games["away_score"] *= scale

    # Round & final columns
    games["home_score"] = games["home_score"].round(2)
    games["away_score"] = games["away_score"].round(2)
    games["total"] = (games["home_score"] + games["away_score"]).round(2)

    # Diagnostics
    print(f"🌦 weather_factor non-1.0 rows: {(games['weather_factor'] != 1.0).sum()} / {len(games)}")
    if hp_col: print(f"🧩 unmatched home_pitcher names: {miss_hp}")
    if ap_col: print(f"🧩 unmatched away_pitcher names: {miss_ap}")

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    cols_out = ["date", h_col, a_col, "home_score", "away_score", "total"]
    # ensure canonical header names
    out = games[["date", h_col, a_col, "home_score", "away_score", "total"]].copy()
    out.columns = ["date","home_team","away_team","home_score","away_score","total"]
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote: {OUTPUT_FILE} (games={len(out)})")

if __name__ == "__main__":
    main()
