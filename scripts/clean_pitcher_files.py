#!/usr/bin/env python3
# Purpose: make the "Starters seen today" count accurate (unique, non-UNKNOWN starters)
# and keep the existing health checks (missing starter IDs per file).

from pathlib import Path
import pandas as pd
from datetime import datetime

CTX_PATH         = Path("data/raw/startingpitchers_with_opp_context.csv")
PROJ_FINAL_PATH  = Path("data/_projections/pitcher_props_projected_final.csv")
MEGA_Z_FINAL_PATH= Path("data/_projections/pitcher_mega_z_final.csv")

def _normalize_id_series(s: pd.Series) -> pd.Series:
    # Robust ID normalization across object/float/int types and "UNKNOWN"
    out = s.astype(str).fillna("UNKNOWN").str.strip()
    # Drop trailing .0 for numeric-looking strings (e.g., "700249.0" -> "700249")
    out = out.str.replace(r"\.0$", "", regex=True)
    # Uppercase literal "unknown"
    out = out.where(out.str.upper() != "UNKNOWN", "UNKNOWN")
    return out

def _read_ids(path: Path, col: str = "player_id") -> pd.Series:
    if not path.exists():
        return pd.Series(dtype="string")
    df = pd.read_csv(path, low_memory=False)
    if col not in df.columns:
        return pd.Series(dtype="string")
    return _normalize_id_series(df[col])

def _unique_non_unknown(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    series = series[series != "UNKNOWN"]
    return pd.Series(series.unique(), dtype="string")

def main():
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f">> START: clean_pitcher_files.py ({ts})")

    # Load today's starters from context
    starters_raw = _read_ids(CTX_PATH, "player_id")
    starters = _unique_non_unknown(starters_raw)
    n_starters = int(starters.nunique()) if not starters.empty else 0
    print(f"Starters seen today: {n_starters}")

    # Check presence in projected_final
    proj_ids = _unique_non_unknown(_read_ids(PROJ_FINAL_PATH, "player_id"))
    missing_proj = sorted(set(starters.tolist()) - set(proj_ids.tolist()))

    proj_rows = 0
    if PROJ_FINAL_PATH.exists():
        proj_rows = len(pd.read_csv(PROJ_FINAL_PATH, low_memory=False))

    print(f"✅ cleaned {PROJ_FINAL_PATH} | rows={proj_rows} | starters missing here={len(missing_proj)}")
    if missing_proj:
        print(f"Missing starter ids in this file: {', '.join(missing_proj)}")

    # Check presence in mega_z_final
    mega_ids = _unique_non_unknown(_read_ids(MEGA_Z_FINAL_PATH, "player_id"))
    missing_mega = sorted(set(starters.tolist()) - set(mega_ids.tolist()))

    mega_rows = 0
    if MEGA_Z_FINAL_PATH.exists():
        mega_rows = len(pd.read_csv(MEGA_Z_FINAL_PATH, low_memory=False))

    print(f"✅ cleaned {MEGA_Z_FINAL_PATH} | rows={mega_rows} | starters missing here={len(missing_mega)}")
    if missing_mega:
        print(f"Missing starter ids in this file: {', '.join(missing_mega)}")

    print(f"[END] clean_pitcher_files.py ({ts})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
