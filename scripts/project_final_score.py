# scripts/project_final_score.py
import pandas as pd
from pathlib import Path

# File paths
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
GAMES_TODAY_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

TARGET_AVG_TOTAL = 9.0  # keep your original scaling target

def _safe_read_csv(p: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(p)
        # normalize column names (strip only)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()

def _norm_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return " ".join(s.strip().split()).lower()

def main():
    batters = _safe_read_csv(BATTER_FILE)
    pitchers = _safe_read_csv(PITCHER_FILE)
    weather = _safe_read_csv(WEATHER_FILE)
    games_today = _safe_read_csv(GAMES_TODAY_FILE)

    if games_today.empty:
        print("❌ games_today file is empty or missing.")
        return

    # Extract date from game_time if present (keeps your output column)
    if "game_time" in games_today.columns:
        games_today["date"] = pd.to_datetime(games_today["game_time"], errors="coerce").dt.date.astype(str)
    else:
        games_today["date"] = ""

    # Build team batter strength: mean ultimate_z per team (fallbacks if column names vary)
    team_col = None
    if "team" in batters.columns:
        team_col = "team"
    elif "Team" in batters.columns:
        team_col = "Team"

    z_col = None
    if "ultimate_z" in batters.columns:
        z_col = "ultimate_z"
    elif "ultimateZ" in batters.columns:
        z_col = "ultimateZ"

    if team_col and z_col and not batters.empty:
        batter_scores = (
            batters[[team_col, z_col]]
            .dropna()
            .groupby(team_col, dropna=False)[z_col]
            .mean()
            .to_dict()
        )
    else:
        batter_scores = {}

    # Pitcher strength map on name -> mega_z (case-insensitive)
    p_name_col = None
    for c in ["name", "Name", "player_name", "last_name, first_name"]:
        if c in pitchers.columns:
            p_name_col = c
            break
    p_z_col = None
    for c in ["mega_z", "megaZ", "z"]:
        if c in pitchers.columns:
            p_z_col = c
            break

    if p_name_col and p_z_col and not pitchers.empty:
        pitcher_scores = {
            _norm_name(n): float(z)
            for n, z in pitchers[[p_name_col, p_z_col]].dropna().itertuples(index=False, name=None)
        }
    else:
        pitcher_scores = {}

    # Weather: keep LEFT join so no game is dropped; default factor = 1.0
    # We'll attempt to match by home+away; if weather has date, you can extend the merge keys.
    merge_keys = ["home_team", "away_team"]
    if not weather.empty:
        weather_cols = [c for c in weather.columns]
        # Normalize: ensure weather_factor exists; if not, create neutral factor
        if "weather_factor" not in weather.columns:
            weather["weather_factor"] = 1.0
        game_data = games_today.merge(
            weather[merge_keys + ["weather_factor"]].drop_duplicates(subset=merge_keys, keep="last"),
            on=merge_keys,
            how="left",
        )
    else:
        game_data = games_today.copy()
        game_data["weather_factor"] = 1.0

    # Fill missing factor
    game_data["weather_factor"] = pd.to_numeric(game_data["weather_factor"], errors="coerce").fillna(1.0).clip(lower=0.5, upper=1.5)

    # Helpers
    def project_side_score(batter_team: str, opp_pitcher: str, weather_factor: float) -> float:
        b = float(batter_scores.get(batter_team, 0.0))
        p = float(pitcher_scores.get(_norm_name(opp_pitcher), 0.0))
        # Linear model: baseline 4.5 runs per team, adjust by batter minus pitcher
        base = 4.5 + (b - p)
        # Apply half of the weather factor to each side so the total reflects the full factor
        per_side_factor = max(0.7, min(1.3, weather_factor ** 0.5))
        score = base * per_side_factor
        # Soft bounds
        return float(max(1.0, min(score, 12.0)))

    rows = []
    for _, row in game_data.iterrows():
        home_team = row.get("home_team", "")
        away_team = row.get("away_team", "")
        factor = float(row.get("weather_factor", 1.0) or 1.0)
        home_pitcher = row.get("pitcher_home", "")
        away_pitcher = row.get("pitcher_away", "")
        date = row.get("date", "")

        home_score = project_side_score(home_team, away_pitcher, factor)
        away_score = project_side_score(away_team, home_pitcher, factor)

        rows.append({
            "home_team": home_team,
            "away_team": away_team,
            "home_pitcher": home_pitcher,
            "away_pitcher": away_pitcher,
            "home_score": home_score,
            "away_score": away_score,
            "weather_factor": factor,
            "date": date
        })

    df = pd.DataFrame(rows)

    # Rescale totals toward league average (your original behavior), only if valid
    if not df.empty:
        totals = df["home_score"] + df["away_score"]
        current_avg = totals.mean()
        if pd.notnull(current_avg) and current_avg > 0:
            scale = TARGET_AVG_TOTAL / current_avg
            df["home_score"] = (df["home_score"] * scale).clip(lower=1.0, upper=15.0)
            df["away_score"] = (df["away_score"] * scale).clip(lower=1.0, upper=15.0)

        # Final rounding
        df["home_score"] = df["home_score"].round(2)
        df["away_score"] = df["away_score"].round(2)
        df["total"] = (df["home_score"] + df["away_score"]).round(2)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Final score projections saved: {OUTPUT_FILE} ({len(df)} games)")

if __name__ == "__main__":
    main()
