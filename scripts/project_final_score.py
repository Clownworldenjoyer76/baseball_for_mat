# scripts/project_final_score.py
import pandas as pd
from pathlib import Path

# -------- File paths (keep these consistent with your workflow) --------
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
GAMES_TODAY_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

TARGET_AVG_TOTAL = 9.0  # target league-wide projected total

# -------- Helpers --------

TEAM_ALIASES = {
    # Common MLB abbreviations/variants -> normalized team key
    "nyy": "yankees", "nya": "yankees", "yanks": "yankees",
    "nym": "mets",
    "chc": "cubs",
    "chw": "white sox", "cws": "white sox", "ws": "white sox",
    "stl": "cardinals",
    "sd": "padres", "sdg": "padres",
    "sf": "giants", "sfg": "giants",
    "tb": "rays", "tbr": "rays",
    "kc": "royals", "kcr": "royals",
    "laa": "angels", "ana": "angels",
    "lad": "dodgers",
    "atl": "braves",
    "bal": "orioles",
    "bos": "red sox", "sox": "red sox",  # NOTE: ambiguous in general; fits your inputs
    "cin": "reds",
    "cle": "guardians", "cws-guardians": "guardians",
    "col": "rockies",
    "det": "tigers",
    "hou": "astros",
    "mia": "marlins",
    "mil": "brewers",
    "min": "twins",
    "oak": "athletics",
    "phi": "phillies",
    "pit": "pirates",
    "sea": "mariners",
    "tex": "rangers",
    "tor": "blue jays",
    "was": "nationals", "wsh": "nationals",
    "ari": "diamondbacks",
}

def _safe_read_csv(p: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(p)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()

def _norm_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return " ".join(s.strip().split()).lower()

def _norm_team(s: str) -> str:
    t = _norm_text(s)
    return TEAM_ALIASES.get(t, t)

# -------- Main --------

def main():
    # Load inputs
    batters = _safe_read_csv(BATTER_FILE)
    pitchers = _safe_read_csv(PITCHER_FILE)
    weather = _safe_read_csv(WEATHER_FILE)
    games = _safe_read_csv(GAMES_TODAY_FILE)

    # Early exit if no games
    if games.empty:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=[
            "home_team","away_team","home_pitcher","away_pitcher","date","game_time",
            "home_score","away_score","total","weather_factor"
        ]).to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Final score projections saved: {OUTPUT_FILE} (0 games)")
        return

    # Normalize team text across all frames to maximize joins/matches
    if "team" in batters.columns:
        batters["team"] = batters["team"].astype(str).map(_norm_team)

    for c in ("home_team","away_team"):
        if c in weather.columns:
            weather[c] = weather[c].astype(str).map(_norm_team)
        if c in games.columns:
            games[c] = games[c].astype(str).map(_norm_team)

    # ---------------- Batter team strength ----------------
    # Use total_bases projection as team batting proxy by default.
    bat_strength = {}
    if not batters.empty and all(col in batters.columns for col in ("team","prop_type","projection")):
        bt = (batters.loc[batters["prop_type"] == "total_bases", ["team","projection"]]
                       .dropna(subset=["team","projection"])
                       .groupby("team", as_index=False)["projection"].mean()
                       .rename(columns={"projection":"bat_strength"}))
        bat_strength = { _norm_team(k): float(v) for k, v in bt.set_index("team")["bat_strength"].items() }

    # ---------------- Pitcher strength (per pitcher name) ----------------
    # Map pitcher name (as in games) -> mega_z from pitcher_mega_z.csv
    pitcher_scores = {}
    if not pitchers.empty:
        # pick name column
        name_col = None
        for c in ["name","Name","player_name","last_name, first_name"]:
            if c in pitchers.columns:
                name_col = c
                break
        z_col = "mega_z" if "mega_z" in pitchers.columns else None

        if name_col and z_col:
            src = pitchers[[name_col, z_col]].dropna()
            pitcher_scores = { _norm_text(n): float(z) for n, z in src.itertuples(index=False, name=None) }

    # ---------------- Weather merge ----------------
    # Default factor to 1.0; left-merge on normalized home/away team names if available
    game_data = games.copy()
    game_data["weather_factor"] = 1.0
    if not weather.empty:
        merge_keys = [k for k in ["home_team","away_team"] if k in weather.columns and k in game_data.columns]
        if merge_keys:
            wcols = list(dict.fromkeys(merge_keys + (["weather_factor"] if "weather_factor" in weather.columns else [])))
            merged = pd.merge(
                game_data, 
                weather[wcols].drop_duplicates(subset=merge_keys),
                on=merge_keys, how="left", suffixes=("","_w")
            )
            if "weather_factor" in merged.columns and "weather_factor_w" in merged.columns:
                merged["weather_factor"] = merged["weather_factor"].fillna(merged["weather_factor_w"])
                merged.drop(columns=["weather_factor_w"], inplace=True, errors="ignore")
            game_data = merged
    # Coerce and clamp
    game_data["weather_factor"] = pd.to_numeric(game_data["weather_factor"], errors="coerce").fillna(1.0).clip(lower=0.7, upper=1.3)

    # ---------------- Scoring model ----------------
    def project_side_score(batter_team: str, opp_pitcher_name: str, weather_factor: float) -> float:
        b = float(bat_strength.get(_norm_team(batter_team), 0.0))
        p = float(pitcher_scores.get(_norm_text(opp_pitcher_name), 0.0))
        # Baseline per-team runs
        base = 4.5 + (b - p)
        # Apply half of weather factor per side to make total roughly reflect full factor
        per_side_factor = max(0.7, min(1.3, weather_factor ** 0.5))
        score = base * per_side_factor
        # Soft bounds
        return float(max(1.0, min(score, 12.0)))

    # Compute scores row-wise
    rows = []
    for _, row in game_data.iterrows():
        home_team = row.get("home_team", "")
        away_team = row.get("away_team", "")
        factor = float(row.get("weather_factor", 1.0) or 1.0)
        home_pitcher = row.get("pitcher_home", "")
        away_pitcher = row.get("pitcher_away", "")
        date = row.get("date", "")  # optional in games file
        game_time = row.get("game_time", "")

        home_score = project_side_score(home_team, away_pitcher, factor)
        away_score = project_side_score(away_team, home_pitcher, factor)

        rows.append({
            "home_team": home_team,
            "away_team": away_team,
            "home_pitcher": home_pitcher,
            "away_pitcher": away_pitcher,
            "date": date,
            "game_time": game_time,
            "home_score": home_score,
            "away_score": away_score,
            "weather_factor": factor,
        })

    df = pd.DataFrame(rows)

    # Optional global scaling to TARGET_AVG_TOTAL (if multiple games)
    if len(df) >= 2:
        current_avg = float((df["home_score"] + df["away_score"]).mean())
        if current_avg > 0:
            scale = TARGET_AVG_TOTAL / current_avg
            df["home_score"] = (df["home_score"] * scale).clip(lower=1.0, upper=15.0)
            df["away_score"] = (df["away_score"] * scale).clip(lower=1.0, upper=15.0)

    # Final rounding & total
    df["home_score"] = df["home_score"].round(2)
    df["away_score"] = df["away_score"].round(2)
    df["total"] = (df["home_score"] + df["away_score"]).round(2)

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Final score projections saved: {OUTPUT_FILE} ({len(df)} games)")

if __name__ == "__main__":
    main()
