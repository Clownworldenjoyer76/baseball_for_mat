#!/usr/bin/env python3
import sys, argparse, unicodedata, re
from pathlib import Path
import pandas as pd

TEAM_NORMALIZATION_MAP = {
    'redsox': 'Red Sox',
    'whitesox': 'White Sox',
    'bluejays': 'Blue Jays',
    'diamondbacks': 'Diamondbacks',
    'braves': 'Braves',
    'cubs': 'Cubs',
    'dodgers': 'Dodgers',
    'mariners': 'Mariners',
    'marlins': 'Marlins',
    'nationals': 'Nationals',
    'padres': 'Padres',
    'phillies': 'Phillies',
    'pirates': 'Pirates',
    'rays': 'Rays',
    'rockies': 'Rockies',
    'tigers': 'Tigers',
    'twins': 'Twins',
}

def normalize_team_column(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower().map(TEAM_NORMALIZATION_MAP).fillna(df[col])
    return df


BATTERS_FILE = Path("data/tagged/batters_normalized.csv")
WEATHER_INPUT  = Path("data/weather_input.csv")        # needs: home_team, Park Factor
WEATHER_ADJUST = Path("data/weather_adjustments.csv")  # needs: home_team, weather_factor

def fail(msg):
    print(f"❌ align_weather_team_keys: {msg}", file=sys.stderr); sys.exit(1)

def norm_team(s: str) -> str:
    s = ("" if pd.isna(s) else str(s)).strip()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def main():
    for p in [BATTERS_FILE, WEATHER_INPUT, WEATHER_ADJUST]:
        if not p.exists():
            fail(f"Missing required file: {p}")

    bat = pd.read_csv(BATTERS_FILE)
    if "team" not in bat.columns:
        fail("batters_normalized.csv must have a 'team' column")
    teams = bat["team"].astype(str).str.strip().unique().tolist()
    if not teams:
        fail("No teams found in batters file")

    canon_map = {norm_team(t): t for t in teams}
    if len(canon_map) < len(teams):
        # collisions after normalization shouldn't really happen for MLB team names
        pass

    wi = pd.read_csv(WEATHER_INPUT)
    wa = pd.read_csv(WEATHER_ADJUST)
    for df, need_cols, label in [(wi, ["home_team","Park Factor"], "weather_input.csv"),
                                 (wa, ["home_team","weather_factor"], "weather_adjustments.csv")]:
        for c in need_cols:
            if c not in df.columns:
                fail(f"{label} missing column: {c}")

    # Align home_team values to canonical batter teams
    def align(df, label):
        df = df.copy()
        df["home_team"] = df["home_team"].astype(str)
        df["_key"] = df["home_team"].map(norm_team)
        df["home_team"] = df["_key"].map(canon_map)
        missing = df["home_team"].isna().sum()
        if missing:
            bad = df[df["home_team"].isna()]["_key"].unique().tolist()
            fail(f"{label}: could not align {missing} rows. Unknown keys: {bad[:10]}")
        df.drop(columns=["_key"], inplace=True)
        return df

    wi2 = align(wi, "weather_input.csv")
    wa2 = align(wa, "weather_adjustments.csv")

    # Validate coverage: all batter teams present in both files after alignment
    if set(teams) - set(wi2["home_team"]):
        miss = sorted(list(set(teams) - set(wi2["home_team"])))
        fail(f"weather_input.csv missing teams after alignment: {miss}")
    if set(teams) - set(wa2["home_team"]):
        miss = sorted(list(set(teams) - set(wa2["home_team"])))
        fail(f"weather_adjustments.csv missing teams after alignment: {miss}")

    wi2.to_csv(WEATHER_INPUT, index=False)
    wa2.to_csv(WEATHER_ADJUST, index=False)
    print(f"✅ align_weather_team_keys: aligned & wrote both weather files. Teams={len(teams)}")

if __name__ == "__main__":
    main()