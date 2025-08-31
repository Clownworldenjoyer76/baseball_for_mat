#!/usr/bin/env python3
# Mobile-safe: enforce Int64 IDs and attach park_factor to todaysgames_normalized.csv

from pathlib import Path
import pandas as pd

ROOT = Path(".")
GAMES_CSV = ROOT / "data/raw/todaysgames_normalized.csv"
TEAMDIR   = ROOT / "data/manual/team_directory.csv"
PF_DAY    = ROOT / "data/manual/park_factors_day.csv"
PF_NIGHT  = ROOT / "data/manual/park_factors_night.csv"
PF_ROOF   = ROOT / "data/manual/park_factors_roof_closed.csv"

def die(msg):
    print("INSUFFICIENT INFORMATION")
    print(msg)
    raise SystemExit(1)

def to_int64(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def hour24(et):
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

def choose_pf(row):
    roof_status = str(row.get("roof_status", "")).strip().lower()
    if roof_status == "closed" and pd.notna(row.get("pf_roof")):
        return row["pf_roof"]
    h = row.get("_hour24")
    if pd.notna(h) and h >= 18 and pd.notna(row.get("pf_night")):
        return row["pf_night"]
    return row.get("pf_day")

def build_alias_maps(td: pd.DataFrame):
    alias_to_abbr, abbr_to_id = {}, {}
    def put(alias, code):
        k = (alias or "").strip().upper()
        v = (code  or "").strip().upper()
        if k and v and k not in alias_to_abbr:
            alias_to_abbr[k] = v
    for _, r in td.iterrows():
        code = (r.get("team_code","") or "").strip().upper()
        tid  = r.get("team_id","")
        if code:
            abbr_to_id[code] = pd.to_numeric(tid, errors="coerce")
        for col in ("team_code","canonical_team","team_name","clean_team_name"):
            put(r.get(col,""), code)
        for name in (r.get("all_names","") or "").split("|"):
            put(name, code)
        for c2 in (r.get("all_codes","") or "").split("|"):
            put(c2, code)
    return alias_to_abbr, abbr_to_id

def norm_team(x, alias_to_abbr):
    key = (x or "").strip().upper()
    return alias_to_abbr.get(key, key)

def main():
    # Require inputs
    for p in (GAMES_CSV, TEAMDIR, PF_DAY, PF_NIGHT, PF_ROOF):
        if not p.exists():
            die(f"Missing file: {p}")

    g = pd.read_csv(GAMES_CSV, dtype=str).fillna("")
    td = pd.read_csv(TEAMDIR, dtype=str).fillna("")
    need = {"team_id","team_code","canonical_team","team_name","clean_team_name","all_codes","all_names"}
    if not need.issubset(td.columns):
        die(f"{TEAMDIR} must include columns: {', '.join(sorted(need))}")

    alias_to_abbr, abbr_to_id = build_alias_maps(td)

    # Normalize abbreviations (idempotent)
    if "home_team" in g.columns:
        g["home_team"] = g["home_team"].map(lambda v: norm_team(v, alias_to_abbr))
    if "away_team" in g.columns:
        g["away_team"] = g["away_team"].map(lambda v: norm_team(v, alias_to_abbr))

    # Rebuild IDs from abbreviations; keep prior if present
    home_id_from_map = g.get("home_team","").map(lambda c: abbr_to_id.get((c or "").strip().upper()))
    away_id_from_map = g.get("away_team","").map(lambda c: abbr_to_id.get((c or "").strip().upper()))

    # Prefer existing IDs if already numeric; else use map
    g["home_team_id"] = pd.Series(
        pd.to_numeric(g.get("home_team_id", pd.Series(index=g.index)), errors="coerce")
    ).where(lambda x: x.notna(), home_id_from_map)
    g["away_team_id"] = pd.Series(
        pd.to_numeric(g.get("away_team_id", pd.Series(index=g.index)), errors="coerce")
    ).where(lambda x: x.notna(), away_id_from_map)

    # Enforce nullable integer to eliminate .0 on write
    g["home_team_id"] = to_int64(g["home_team_id"])
    g["away_team_id"] = to_int64(g["away_team_id"])

    # Attach park factors
    pf_day  = pd.read_csv(PF_DAY)
    pf_ngt  = pd.read_csv(PF_NIGHT)
    pf_roof = pd.read_csv(PF_ROOF)
    for pf in (pf_day, pf_ngt, pf_roof):
        pf["team_id"] = to_int64(pf["team_id"])

    merged = g.copy()
    # Need hour for day/night decide
    if "game_time" in merged.columns:
        merged["_hour24"] = merged["game_time"].map(hour24)
    else:
        merged["_hour24"] = pd.NA

    merged = merged.merge(pf_day[["team_id","Park Factor"]].rename(columns={"Park Factor":"pf_day"}),
                          left_on="home_team_id", right_on="team_id", how="left").drop(columns=["team_id"])
    merged = merged.merge(pf_ngt[["team_id","Park Factor"]].rename(columns={"Park Factor":"pf_night"}),
                          left_on="home_team_id", right_on="team_id", how="left").drop(columns=["team_id"])
    merged = merged.merge(pf_roof[["team_id","Park Factor"]].rename(columns={"Park Factor":"pf_roof"}),
                          left_on="home_team_id", right_on="team_id", how="left").drop(columns=["team_id"])

    merged["park_factor"] = merged.apply(choose_pf, axis=1)
    merged = merged.drop(columns=["pf_day","pf_night","pf_roof","_hour24"], errors="ignore")

    # Preserve original column order; append ids/pf if missing
    cols = list(g.columns)
    for c in ("home_team_id","away_team_id","park_factor"):
        if c not in cols:
            cols.append(c)
    out = merged[cols]

    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(GAMES_CSV, index=False)
    filled_ids = int(out["home_team_id"].notna().sum())
    filled_pf  = int(out["park_factor"].notna().sum()) if "park_factor" in out.columns else 0
    print(f"✅ Wrote {GAMES_CSV} | IDs filled: {filled_ids}/{len(out)} | park_factor filled: {filled_pf}/{len(out)}")

if __name__ == "__main__":
    main()
