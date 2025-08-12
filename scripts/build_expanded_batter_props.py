#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Inputs
BATS_PROJ = Path("data/_projections/batter_props_projected.csv")
HOME_ADJ = Path("data/adjusted/batters_home_adjusted.csv")
AWAY_ADJ = Path("data/adjusted/batters_away_adjusted.csv")

# Output
OUT = Path("data/_projections/batter_props_z_expanded.csv")

TEAM_FIX = {
    "redsox": "Red Sox",
    "whitesox": "White Sox",
    "bluejays": "Blue Jays",
    "diamondbacks": "Diamondbacks",
    "braves": "Braves",
    "cubs": "Cubs",
    "dodgers": "Dodgers",
    "mariners": "Mariners",
    "marlins": "Marlins",
    "nationals": "Nationals",
    "padres": "Padres",
    "phillies": "Phillies",
    "pirates": "Pirates",
    "rays": "Rays",
    "rockies": "Rockies",
    "tigers": "Tigers",
    "twins": "Twins",
    "white sox": "White Sox",
    "red sox": "Red Sox",
    "blue jays": "Blue Jays",
}

def norm_team(s: pd.Series) -> pd.Series:
    base = s.astype(str).str.strip()
    key = base.str.lower().str.replace(" ", "", regex=False)
    fixed = key.map(TEAM_FIX).fillna(base.str.title())
    return fixed

def extract_team_factors(df: pd.DataFrame, team_col: str) -> pd.DataFrame:
    cols = df.columns.str.strip()
    df.columns = cols
    # Accept both 'Park Factor' and 'ParkFactor' and any weather_factor case
    park_col = next((c for c in df.columns if c.replace(" ", "").lower() == "parkfactor"), None)
    wf_col = next((c for c in df.columns if c.lower() == "weather_factor"), None)
    if park_col is None:
        # Some files store park factor as 'adj_woba_park' multiplier — fallback to 1.0
        df["ParkFactor"] = 1.0
        park_col = "ParkFactor"
    if wf_col is None:
        df["weather_factor"] = 1.0
        wf_col = "weather_factor"
    out = df[[team_col, park_col, wf_col]].copy()
    out.columns = ["team", "park_factor", "weather_factor"]
    out["team"] = norm_team(out["team"])
    # Keep last seen values per team
    out = out.groupby("team", as_index=False).last()
    return out

def main():
    bats = pd.read_csv(BATS_PROJ)
    bats.columns = bats.columns.str.strip()
    # Normalize team for bats
    team_col = "team" if "team" in bats.columns else None
    if team_col is None:
        # fallback if using home/away flags; derive from 'team' in source
        raise SystemExit("❌ Expected 'team' column in batter_props_projected.csv")
    bats["team"] = norm_team(bats["team"])

    # Build factor table from home + away adjusted files
    home = pd.read_csv(HOME_ADJ)
    away = pd.read_csv(AWAY_ADJ)

    # Home file has 'home_team', away has 'away_team'
    home_f = extract_team_factors(home, "home_team" if "home_team" in home.columns else "team")
    away_f = extract_team_factors(away, "away_team" if "away_team" in away.columns else "team")
    factors = pd.concat([home_f, away_f], ignore_index=True).groupby("team", as_index=False).last()

    # Merge
    merged = bats.merge(factors, on="team", how="left", validate="m:1")

    # Fill any remaining gaps conservatively
    missing = merged["park_factor"].isna().sum() + merged["weather_factor"].isna().sum()
    if missing:
        # Try a second pass: remove spaces from team and try again
        alt = factors.copy()
        alt["team_alt"] = alt["team"].str.replace(" ", "", regex=False).str.lower()
        merged["team_alt"] = merged["team"].str.replace(" ", "", regex=False).str.lower()
        merged = merged.merge(alt.drop(columns=["team"]).rename(columns={"team_alt": "team_alt_f"}),
                              left_on="team_alt", right_on="team_alt_f", how="left", suffixes=("", "_alt"))
        merged["park_factor"] = merged["park_factor"].fillna(merged["park_factor_alt"])
        merged["weather_factor"] = merged["weather_factor"].fillna(merged["weather_factor_alt"])
        merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_alt")])

    merged["park_factor"] = merged["park_factor"].fillna(1.0)
    merged["weather_factor"] = merged["weather_factor"].fillna(1.0)

    # Optional combined adjustment if needed later
    merged["combined_factor"] = merged["park_factor"] * merged["weather_factor"]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT, index=False)
    print(f"✅ Wrote expanded batter props → {OUT} (rows={len(merged)})")

if __name__ == "__main__":
    main()
