#!/usr/bin/env python3
# scripts/project_final_score.py  (ID-based pitcher strength)
import pandas as pd
import numpy as np
from pathlib import Path

TARGET_AVG_TOTAL = 9.0
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")  # must have player_id, mega_z
GAMES_CANDIDATES = [
    Path("data/cleaned/games_today_cleaned.csv"),
    Path("data/end_chain/cleaned/games_today_cleaned.csv"),
    Path("data/raw/todaysgames_normalized.csv"),
]
WEATHER_FILE = Path("data/weather_adjustments.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def _pick_first_existing(paths):
    for p in paths:
        if p.exists():
            return p
    return None

def _norm_team(x): return str(x).strip().lower()

def _pick_col(df, options):
    for c in options:
        if c in df.columns:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in options:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def load_strengths():
    bat = pd.read_csv(BATTER_FILE)
    if not {"team","prop_type","projection"}.issubset(bat.columns):
        raise SystemExit("❌ batter file missing columns: team, prop_type, projection")
    bat["team"] = bat["team"].map(_norm_team)
    bats_tb = bat[bat["prop_type"] == "total_bases"].copy()
    bat_strength = (
        bats_tb.groupby("team")["projection"]
        .apply(lambda s: float(np.mean(sorted(s, reverse=True)[:9])) if len(s) else 0.0)
        .to_dict()
    )

    pit = pd.read_csv(PITCHER_FILE)
    if not {"player_id","mega_z"}.issubset(pit.columns):
        raise SystemExit("❌ pitcher file must contain 'player_id' and 'mega_z'")
    pit["player_id"] = pit["player_id"].astype(str).str.strip()
    pitch_strength = pit.dropna(subset=["player_id"]).drop_duplicates("player_id").set_index("player_id")["mega_z"].to_dict()

    print(f"📊 Loaded strengths → bat_teams={len(bat_strength)} pitcher_ids={len(pitch_strength)}")
    return bat_strength, pitch_strength

def main():
    bat_strength, pitch_strength = load_strengths()

    games_path = _pick_first_existing(GAMES_CANDIDATES)
    if games_path is None:
        raise SystemExit("❌ No games file found.")
    games = pd.read_csv(games_path)
    print(f"🧾 Using games file: {games_path} (rows={len(games)})")

    # Resolve required columns
    h_col = _pick_col(games, ["home_team","home"])
    a_col = _pick_col(games, ["away_team","away"])
    hpid_col = _pick_col(games, ["home_pitcher_id"])
    apid_col = _pick_col(games, ["away_pitcher_id"])
    date_col = _pick_col(games, ["date","game_date"])
    if not (h_col and a_col):
        raise SystemExit("❌ games file missing home_team/away_team columns")
    if not (hpid_col and apid_col):
        raise SystemExit("❌ games file missing pitcher IDs. Run scripts/attach_pitcher_ids.py first.")

    games["home_team_norm"] = games[h_col].map(_norm_team)
    games["away_team_norm"] = games[a_col].map(_norm_team)
    games["home_pitcher_id"] = games[hpid_col].astype(str).str.strip()
    games["away_pitcher_id"] = games[apid_col].astype(str).str.strip()

    # Date
    if date_col and games[date_col].notna().any():
        games["date"] = games[date_col].fillna(method="ffill").fillna(method="bfill").astype(str)
    else:
        games["date"] = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Weather
    if WEATHER_FILE.exists():
        wx = pd.read_csv(WEATHER_FILE)
        if {"home_team","weather_factor"}.issubset(wx.columns):
            wx = wx.copy()
            wx["home_team_norm"] = wx["home_team"].map(_norm_team)
            wx["weather_factor"] = pd.to_numeric(wx["weather_factor"], errors="coerce")
            games = games.merge(wx[["home_team_norm","weather_factor"]], on="home_team_norm", how="left")
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

        ph = pitch_strength.get(r["home_pitcher_id"], 0.0)
        pa = pitch_strength.get(r["away_pitcher_id"], 0.0)
        if ph == 0.0: miss_hp += 1
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

    if len(games) >= 6 and (avg_total < 8.2 or avg_total > 9.8):
        scale = TARGET_AVG_TOTAL / avg_total
        games["home_score"] *= scale
        games["away_score"] *= scale

    games["home_score"] = games["home_score"].round(2)
    games["away_score"] = games["away_score"].round(2)
    games["total"] = (games["home_score"] + games["away_score"]).round(2)

    print(f"🧩 unmatched pitcher IDs → home: {miss_hp}, away: {miss_ap}")
    print(f"🌦 weather_factor non-1.0 rows: {(games['weather_factor'] != 1.0).sum()} / {len(games)}")

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out = games[["date", h_col, a_col, "home_score", "away_score", "total"]].copy()
    out.columns = ["date","home_team","away_team","home_score","away_score","total"]
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote: {OUTPUT_FILE} (games={len(out)})")

if __name__ == "__main__":
    main()
