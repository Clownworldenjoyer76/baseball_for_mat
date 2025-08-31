# scripts/update_game_time.py
#!/usr/bin/env python3
import sys
import math
from pathlib import Path
from datetime import datetime
import pandas as pd

GAMES_CSV   = Path("data/raw/todaysgames_normalized.csv")
STADIUM_CSV = Path("data/Data/stadium_metadata.csv")

PF_DAY_CSV        = Path("data/manual/park_factors_day.csv")
PF_NIGHT_CSV      = Path("data/manual/park_factors_night.csv")
PF_ROOF_CLOSED_CSV= Path("data/manual/park_factors_roof_closed.csv")

# ---------- helpers ----------
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df

def _find_col(df: pd.DataFrame, candidates) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def _parse_time_to_hour_et(s: str) -> int | None:
    """
    Accepts strings like '1:05 PM' or '13:05' and returns hour [0..23] ET.
    If not parseable -> None.
    """
    if not isinstance(s, str):
        return None
    t = s.strip()
    if not t:
        return None
    for fmt in ("%I:%M %p", "%H:%M", "%I %p", "%H"):
        try:
            dt = datetime.strptime(t, fmt)
            return dt.hour
        except Exception:
            continue
    return None

def _is_day_game(game_time: str) -> bool | None:
    """
    Day if hour in [10..17] inclusive by convention.
    Returns None if unknown.
    """
    hr = _parse_time_to_hour_et(game_time)
    if hr is None:
        return None
    return 10 <= hr <= 17

def _coerce_float(x):
    try:
        f = float(x)
        if math.isfinite(f):
            return f
    except Exception:
        pass
    return None

def _load_pf_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df = _norm_cols(df)

    # detect team key
    team_col = _find_col(
        df,
        ["team","team_name","home_team","mlb_team","abbreviation","code","team_abbreviation"]
    )
    if team_col is None:
        # try to salvage by using first column
        team_col = df.columns[0] if len(df.columns) else None
    # detect park factor numeric column
    pf_col = _find_col(
        df,
        ["park_factor","parkfactor","park_factors","park","factor","park_factor_index","park_factor_run"]
    )
    if pf_col is None:
        # fallback: try second column
        pf_col = df.columns[1] if len(df.columns) > 1 else None

    if team_col is None or pf_col is None:
        return pd.DataFrame()

    out = df[[team_col, pf_col]].copy()
    out.columns = ["team_key", "park_factor"]
    out["team_key"] = out["team_key"].astype(str).str.strip().str.upper()
    out["park_factor"] = out["park_factor"].apply(_coerce_float)
    out = out.dropna(subset=["team_key","park_factor"])
    return out

def _detect_roof_closed(stadium_row: pd.Series) -> bool:
    """
    Detect roof-closed using flexible fields:
    - is_dome (bool-like)
    - roof / roof_status (strings: 'closed','open', etc.)
    - notes may contain 'roof closed'
    """
    sr = {k: stadium_row.get(k) for k in stadium_row.index}

    # boolean-ish dome
    for key in ("is_dome","isdome","dome"):
        v = sr.get(key)
        if isinstance(v, str):
            if v.strip().lower() in {"true","1","yes","y"}:
                return True
        elif pd.notna(v) and bool(v):
            return True

    # explicit roof status
    for key in ("roof","roof_status","roofstate","roof_mode"):
        v = sr.get(key)
        if isinstance(v, str) and v.strip().lower() in {"closed","close","shut"}:
            return True

    # notes text
    for key in ("notes","comment"):
        v = sr.get(key)
        if isinstance(v, str) and "roof" in v.lower() and "closed" in v.lower():
            return True

    return False

# ---------- main ----------
def main():
    # load inputs
    if not GAMES_CSV.exists():
        print(f"ERROR: missing {GAMES_CSV}", file=sys.stderr)
        sys.exit(1)

    games = pd.read_csv(GAMES_CSV)
    games = _norm_cols(games)

    # require minimal columns
    home_col = _find_col(games, ["home_team","home","home_abbr","hometeam"])
    time_col = _find_col(games, ["game_time","time","start_time","scheduled"])
    if home_col is None:
        print("ERROR: games file missing home_team column.", file=sys.stderr)
        sys.exit(1)
    # time is optional; if missing we’ll treat as night (conservative)
    if time_col is None:
        time_col = None

    # standardize home key
    games["__home_key__"] = games[home_col].astype(str).str.strip().str.upper()

    # stadium metadata (optional but used for roof detection)
    stad = pd.DataFrame()
    if STADIUM_CSV.exists():
        stad = pd.read_csv(STADIUM_CSV)
        stad = _norm_cols(stad)
        # discover team join key
        stad_team = _find_col(stad, ["team","team_name","home_team","abbreviation","code","team_abbreviation"])
        if stad_team is None and len(stad.columns):
            stad_team = stad.columns[0]
        if stad_team is not None:
            stad["__home_key__"] = stad[stad_team].astype(str).str.strip().str.upper()
            # keep only fields helpful for roof detection
            keep = ["__home_key__","is_dome","isdome","dome","roof","roof_status","roofstate","roof_mode","notes","comment"]
            keep = [c for c in keep if c in stad.columns]
            stad = stad[keep].drop_duplicates("__home_key__", keep="first")
        else:
            stad = pd.DataFrame()

    # load park factor tables
    pf_day   = _load_pf_table(PF_DAY_CSV)
    pf_night = _load_pf_table(PF_NIGHT_CSV)
    pf_roof  = _load_pf_table(PF_ROOF_CLOSED_CSV)

    if pf_day.empty and pf_night.empty and pf_roof.empty:
        print("ERROR: no usable park factor tables found in data/manual/.", file=sys.stderr)
        sys.exit(1)

    # decide source per game
    def _pick_pf_source(row) -> str:
        # roof check via stadium metadata if available
        if not stad.empty:
            srow = stad.loc[stad["__home_key__"] == row["__home_key__"]]
            if not srow.empty and _detect_roof_closed(srow.iloc[0]):
                return "roof"
        # if no roof-closed, use day/night from game time (if available)
        if time_col is not None:
            is_day = _is_day_game(str(row[time_col]))
            if is_day is True:
                return "day"
            if is_day is False:
                return "night"
        # fallback: night
        return "night"

    games["__pf_source__"] = games.apply(_pick_pf_source, axis=1)

    # attach factor
    games["Park Factor"] = None

    # merge helpers
    def _attach(gdf, src_name, pf_df):
        if pf_df.empty:
            return gdf
        subset = gdf[gdf["__pf_source__"] == src_name].copy()
        if subset.empty:
            return gdf
        merged = subset.merge(
            pf_df.rename(columns={"team_key":"__home_key__"}),
            on="__home_key__", how="left"
        )
        # write back
        gdf.loc[merged.index, "Park Factor"] = merged["park_factor"].where(
            pd.notna(merged["park_factor"]), gdf.loc[merged.index, "Park Factor"]
        )
        return gdf

    games = _attach(games, "roof",  pf_roof)
    games = _attach(games, "day",   pf_day)
    games = _attach(games, "night", pf_night)

    # finalize
    games.drop(columns=["__home_key__","__pf_source__"], inplace=True, errors="ignore")
    # keep original column order; if Park Factor new, append
    if "Park Factor" not in games.columns:
        games["Park Factor"] = None

    # save back
    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_CSV, index=False)
    print(f"✅ Updated park factors in {GAMES_CSV}")

if __name__ == "__main__":
    main()
