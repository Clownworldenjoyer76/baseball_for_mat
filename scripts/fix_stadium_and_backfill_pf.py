#!/usr/bin/env python3
# Mobile-safe: fix ID dtypes and backfill Park Factor into stadium_metadata.csv

from pathlib import Path
import pandas as pd

# ---- Paths (repo-relative) ----
ROOT = Path(".")
STADIUM_CSV = ROOT / "data/Data/stadium_metadata.csv"
PF_DAY_CSV  = ROOT / "data/manual/park_factors_day.csv"
PF_NGT_CSV  = ROOT / "data/manual/park_factors_night.csv"
PF_ROOF_CSV = ROOT / "data/manual/park_factors_roof_closed.csv"

# ---- Helpers ----
def _need(paths):
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        print("INSUFFICIENT INFORMATION")
        print("Missing:", ", ".join(missing))
        return True
    return False

def _to_int64(x):
    return pd.to_numeric(x, errors="coerce").astype("Int64")

def _hour24(et):
    """
    Convert 'H:MM AM/PM' to 0–23. Returns <NA> on parse failure.
    """
    try:
        t = str(et).strip()
        if not t or t.lower() == "nan":
            return pd.NA
        hh, rest = t.split(":", 1)
        mm, ampm = rest.split(" ")
        h = int(hh) % 12
        if ampm.upper() == "PM":
            h += 12
        return h
    except Exception:
        return pd.NA

def _choose_pf(row):
    roof_status = str(row.get("roof_status", "")).strip().lower()
    if roof_status == "closed" and pd.notna(row.get("pf_roof")):
        return row["pf_roof"]
    h = row.get("_hour24")
    if pd.notna(h) and h >= 18 and pd.notna(row.get("pf_night")):
        return row["pf_night"]
    return row.get("pf_day")

# ---- Main ----
def main():
    # Require all inputs
    if _need([STADIUM_CSV, PF_DAY_CSV, PF_NGT_CSV, PF_ROOF_CSV]):
        return

    s = pd.read_csv(STADIUM_CSV)

    # Enforce integer IDs to avoid decimals
    if "team_id" in s.columns:
        s["team_id"] = _to_int64(s["team_id"])
    else:
        print("INSUFFICIENT INFORMATION")
        print("Missing column: team_id in stadium file")
        return

    if "home_team_id" in s.columns:
        s["home_team_id"] = _to_int64(s["home_team_id"])

    # Load manual PF tables
    pf_day  = pd.read_csv(PF_DAY_CSV)
    pf_ngt  = pd.read_csv(PF_NGT_CSV)
    pf_roof = pd.read_csv(PF_ROOF_CSV)

    for pf in (pf_day, pf_ngt, pf_roof):
        if "team_id" not in pf.columns or "Park Factor" not in pf.columns:
            print("INSUFFICIENT INFORMATION")
            print("Manual PF files must have columns: team_id, Park Factor")
            return
        pf["team_id"] = _to_int64(pf["team_id"])

    # Merge PFs by numeric team_id
    m = s.merge(
        pf_day[["team_id", "Park Factor"]].rename(columns={"Park Factor": "pf_day"}),
        on="team_id",
        how="left",
    )
    m = m.merge(
        pf_ngt[["team_id", "Park Factor"]].rename(columns={"Park Factor": "pf_night"}),
        on="team_id",
        how="left",
    )
    m = m.merge(
        pf_roof[["team_id", "Park Factor"]].rename(columns={"Park Factor": "pf_roof"}),
        on="team_id",
        how="left",
    )

    # Derive hour for night/day selection (optional if game_time exists)
    if "game_time" in m.columns:
        m["_hour24"] = m["game_time"].map(_hour24)
    else:
        m["_hour24"] = pd.NA

    # Select final Park Factor
    m["Park Factor"] = m.apply(_choose_pf, axis=1)

    # Drop helper
    m = m.drop(columns=["_hour24"], errors="ignore")

    # Preserve original column order; append Park Factor if absent
    cols = list(s.columns)
    if "Park Factor" not in cols:
        cols.append("Park Factor")
    out = m[cols]

    # Write back
    STADIUM_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(STADIUM_CSV, index=False)

    filled = out["Park Factor"].notna().sum() if "Park Factor" in out.columns else 0
    total = len(out)
    print(f"✅ Wrote {STADIUM_CSV} | Park Factor filled {filled}/{total}")

if __name__ == "__main__":
    main()
