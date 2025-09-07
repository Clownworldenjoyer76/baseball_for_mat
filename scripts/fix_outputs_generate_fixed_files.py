#!/usr/bin/env python3
"""
Goal
----
Backfill missing starter IDs in data/raw/todaysgames_normalized.csv using
authoritative per-game starters (26 rows) before falling back to master/season.

Priority of sources:
  1) data/raw/startingpitchers_with_opp_context.csv  (authoritative)
  2) data/processed/player_team_master.csv           (fallback by name)
  3) data/Data/pitchers.csv                          (fallback by name)

Outputs on success:
  - data/_projections/todaysgames_normalized_fixed.csv  (IDs kept as Int64)

Outputs on failure (exit 1):
  - summaries/projections/missing_pitcher_ids.csv
  - summaries/projections/missing_master_columns.txt (if structure problems)

This script NEVER fabricates IDs; it only maps from your real sources.
"""

from pathlib import Path
import sys
import unicodedata
import pandas as pd

RAW_TODAY   = Path("data/raw/todaysgames_normalized.csv")
STARTERS_26 = Path("data/raw/startingpitchers_with_opp_context.csv")
MASTER      = Path("data/processed/player_team_master.csv")
SEASON_P    = Path("data/Data/pitchers.csv")
SUM_DIR     = Path("summaries/projections")
OUT_DIR     = Path("data/_projections")

SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- helpers ----------

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def normalize_name(val) -> str:
    if not isinstance(val, str):
        return ""
    s = _strip_accents(val).lower()
    for ch in [".", ",", "'", "\"", "’", "`", "´", "-", "–", "—", "(", ")", "/"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    # common particles/concat
    s = s.replace(" de grom", " degrom")
    s = s.replace(" de la ", " dela ")
    return s

def to_intlike(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def find_id_col(df: pd.DataFrame) -> str | None:
    for c in ["player_id", "pitcher_id", "mlb_id", "person_id", "id"]:
        if c in df.columns:
            return c
    # if not, try any column ending with _id that isn't game_id/team_id
    for c in df.columns:
        lc = c.lower()
        if lc.endswith("_id") and lc not in {"game_id", "team_id", "home_team_id", "away_team_id"}:
            return c
    return None

def find_name_col(df: pd.DataFrame) -> str | None:
    for c in ["player_name", "pitcher_name", "name", "full_name", "mlb_name", "display_name"]:
        if c in df.columns:
            return c
    return None

def build_name_norm(df: pd.DataFrame, new_col: str = "name_norm") -> bool:
    c = find_name_col(df)
    if c:
        df[new_col] = df[c].map(normalize_name)
        return True
    # try first/last
    first_opts = ["first_name", "firstname", "given_name"]
    last_opts  = ["last_name", "lastname", "family_name", "surname"]
    first = next((x for x in first_opts if x in df.columns), None)
    last  = next((x for x in last_opts  if x in df.columns), None)
    if first and last:
        df[new_col] = (df[first].fillna("") + " " + df[last].fillna("")).map(normalize_name)
        return True
    return False

def write_text(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8")

# ---------- core logic ----------

def map_from_starters_26(schedule: pd.DataFrame) -> pd.DataFrame:
    """
    Use data/raw/startingpitchers_with_opp_context.csv to fill pitcher_home_id / pitcher_away_id.
    Supports both wide (home/away columns) and long (one row per starter) shapes.
    """
    if not STARTERS_26.exists():
        return schedule  # nothing to do

    sp = pd.read_csv(STARTERS_26)
    # Ensure game_id present
    if "game_id" not in sp.columns:
        write_text(SUM_DIR / "missing_master_columns.txt",
                   "startingpitchers_with_opp_context.csv is missing game_id")
        return schedule

    # CASE A: WIDE shape (has pitcher_home_id / pitcher_away_id or pitcher_home / pitcher_away)
    wide_id_home = next((c for c in sp.columns if c.lower() in {"pitcher_home_id","home_pitcher_id"}), None)
    wide_id_away = next((c for c in sp.columns if c.lower() in {"pitcher_away_id","away_pitcher_id"}), None)
    wide_nm_home = next((c for c in sp.columns if c.lower() in {"pitcher_home","home_pitcher","home_name"}), None)
    wide_nm_away = next((c for c in sp.columns if c.lower() in {"pitcher_away","away_pitcher","away_name"}), None)

    if wide_id_home or wide_id_away or wide_nm_home or wide_nm_away:
        sp_w = sp.copy()
        if wide_id_home:
            sp_w[wide_id_home] = to_intlike(sp_w[wide_id_home])
        if wide_id_away:
            sp_w[wide_id_away] = to_intlike(sp_w[wide_id_away])

        # merge and fill by IDs where schedule is missing
        sch = schedule.copy()
        sch = sch.merge(sp_w[["game_id"] + [c for c in [wide_id_home, wide_id_away] if c]],
                        on="game_id", how="left", suffixes=("", "_sp"))
        # fill IDs directly if present in starters file
        if wide_id_home:
            sch["pitcher_home_id"] = sch["pitcher_home_id"].astype("Int64").fillna(sch[wide_id_home])
        if wide_id_away:
            sch["pitcher_away_id"] = sch["pitcher_away_id"].astype("Int64").fillna(sch[wide_id_away])

        # optional: name-based assist (if IDs still missing and names present)
        if (sch["pitcher_home_id"].isna().any() or sch["pitcher_away_id"].isna().any()) and (wide_nm_home or wide_nm_away):
            if wide_nm_home:
                sch["sp_home_norm"] = sch[wide_nm_home].map(normalize_name)
            if wide_nm_away:
                sch["sp_away_norm"] = sch[wide_nm_away].map(normalize_name)
            sch["pitcher_home_norm"] = sch["pitcher_home"].map(normalize_name)
            sch["pitcher_away_norm"] = sch["pitcher_away"].map(normalize_name)
            # if names match, but id missing on schedule and present in sp file, copy
            if wide_id_home and wide_nm_home:
                mask = sch["pitcher_home_id"].isna() & (sch["pitcher_home_norm"] == sch["sp_home_norm"])
                sch.loc[mask, "pitcher_home_id"] = sch.loc[mask, wide_id_home]
            if wide_id_away and wide_nm_away:
                mask = sch["pitcher_away_id"].isna() & (sch["pitcher_away_norm"] == sch["sp_away_norm"])
                sch.loc[mask, "pitcher_away_id"] = sch.loc[mask, wide_id_away]

        # cleanup helper cols
        drop_cols = [c for c in ["sp_home_norm","sp_away_norm","pitcher_home_norm","pitcher_away_norm", wide_id_home, wide_id_away] if c in sch.columns]
        sch = sch.drop(columns=[c for c in drop_cols if c not in {"pitcher_home_id","pitcher_away_id"}], errors="ignore")
        return sch

    # CASE B: LONG shape (one row per starter with team_id + id + name)
    id_col = find_id_col(sp)
    if not id_col:
        # cannot help
        return schedule

    # attempt to infer side from team match against schedule
    sp_long = sp.copy()
    sp_long[id_col] = to_intlike(sp_long[id_col])

    # try to find a team_id column on starters
    sp_team_col = None
    for c in ["team_id","pitcher_team_id","mlb_team_id","tm_id"]:
        if c in sp_long.columns:
            sp_team_col = c
            break

    # best-effort join by (game_id, team_id -> home/away)
    sch = schedule.copy()
    sch["pitcher_home_id"] = sch["pitcher_home_id"].astype("Int64")
    sch["pitcher_away_id"] = sch["pitcher_away_id"].astype("Int64")

    if sp_team_col:
        # join for home side
        m_home = sch.merge(
            sp_long[["game_id", sp_team_col, id_col]],
            left_on=["game_id", "home_team_id"],
            right_on=["game_id", sp_team_col],
            how="left"
        ).rename(columns={id_col: "id_from_sp_home"})
        # join for away side
        m_away = m_home.merge(
            sp_long[["game_id", sp_team_col, id_col]],
            left_on=["game_id", "away_team_id"],
            right_on=["game_id", sp_team_col],
            how="left"
        ).rename(columns={id_col: "id_from_sp_away"})

        m_away["pitcher_home_id"] = m_away["pitcher_home_id"].fillna(m_away["id_from_sp_home"])
        m_away["pitcher_away_id"] = m_away["pitcher_away_id"].fillna(m_away["id_from_sp_away"])
        return m_away.drop(columns=[col for col in m_away.columns if col.endswith("_from_sp_home") or col.endswith("_from_sp_away") or col == sp_team_col])

    # If no team_id on starters file, try name-based as a last resort within the game_id
    ok_name = build_name_norm(sp_long, new_col="name_norm")
    if ok_name:
        sch = schedule.copy()
        sch["pitcher_home_norm"] = sch["pitcher_home"].map(normalize_name)
        sch["pitcher_away_norm"] = sch["pitcher_away"].map(normalize_name)
        # choose any name column from starters
        name_col = "name_norm"
        # home
        tmp = sp_long[["game_id", name_col, id_col]].copy().rename(columns={id_col:"pid"})
        m = sch.merge(tmp, left_on=["game_id","pitcher_home_norm"], right_on=["game_id", name_col], how="left")
        m["pitcher_home_id"] = m["pitcher_home_id"].astype("Int64").fillna(to_intlike(m["pid"]))
        m = m.drop(columns=["pid", name_col], errors="ignore")
        # away
        tmp = sp_long[["game_id", "name_norm", id_col]].copy().rename(columns={id_col:"pid"})
        m = m.merge(tmp, left_on=["game_id","pitcher_away_norm"], right_on=["game_id","name_norm"], how="left", suffixes=("","_2"))
        m["pitcher_away_id"] = m["pitcher_away_id"].astype("Int64").fillna(to_intlike(m["pid"]))
        m = m.drop(columns=["pid", "name_norm", "name_norm_2"], errors="ignore")
        return m.drop(columns=["pitcher_home_norm","pitcher_away_norm"], errors="ignore")

    return schedule

def build_name_maps() -> pd.DataFrame:
    """Combine MASTER and SEASON_P into a name_norm -> player_id map."""
    maps = []

    if MASTER.exists():
        df = pd.read_csv(MASTER)
        ok_name = build_name_norm(df, "name_norm")
        id_col = find_id_col(df)
        if ok_name and id_col:
            m = df[["name_norm", id_col]].dropna().copy()
            m[id_col] = to_intlike(m[id_col])
            maps.append(m.rename(columns={id_col: "player_id"}))
        else:
            msg = []
            if not ok_name: msg.append("player_team_master.csv: no recognizable name fields.")
            if not id_col:  msg.append("player_team_master.csv: no recognizable player_id column.")
            if msg:
                prev = (SUM_DIR / "missing_master_columns.txt").read_text("utf-8") if (SUM_DIR / "missing_master_columns.txt").exists() else ""
                write_text(SUM_DIR / "missing_master_columns.txt", prev + ("\n" if prev else "") + " | ".join(msg))

    if SEASON_P.exists():
        df = pd.read_csv(SEASON_P)
        ok_name = build_name_norm(df, "name_norm")
        id_col = find_id_col(df)
        if ok_name and id_col:
            m = df[["name_norm", id_col]].dropna().copy()
            m[id_col] = to_intlike(m[id_col])
            maps.append(m.rename(columns={id_col: "player_id"}))
        else:
            msg = []
            if not ok_name: msg.append("pitchers.csv: no recognizable name fields.")
            if not id_col:  msg.append("pitchers.csv: no recognizable player_id column.")
            if msg:
                prev = (SUM_DIR / "missing_master_columns.txt").read_text("utf-8") if (SUM_DIR / "missing_master_columns.txt").exists() else ""
                write_text(SUM_DIR / "missing_master_columns.txt", prev + ("\n" if prev else "") + " | ".join(msg))

    if maps:
        out = pd.concat(maps, ignore_index=True).dropna().drop_duplicates()
        out["player_id"] = to_intlike(out["player_id"])
        return out
    return pd.DataFrame(columns=["name_norm", "player_id"])

def main() -> None:
    if not RAW_TODAY.exists():
        write_text(SUM_DIR / "missing_master_columns.txt", f"Missing input: {RAW_TODAY}")
        print(f"❌ Missing input: {RAW_TODAY}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(RAW_TODAY)
    # Validate core columns
    req = ["game_id","home_team_id","away_team_id","pitcher_home","pitcher_away","pitcher_home_id","pitcher_away_id"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        write_text(SUM_DIR / "missing_master_columns.txt", f"todaysgames_normalized.csv missing columns: {miss}")
        print(f"❌ Schedule missing columns: {miss}", file=sys.stderr)
        sys.exit(1)

    # Prepare schedule
    df["pitcher_home_id"] = to_intlike(df["pitcher_home_id"])
    df["pitcher_away_id"] = to_intlike(df["pitcher_away_id"])

    # 1) Authoritative per-game patch from 26-row starters file
    df = map_from_starters_26(df)

    # 2) Fallback by name from master/season for any residual gaps
    need_home = df["pitcher_home_id"].isna()
    need_away = df["pitcher_away_id"].isna()

    if need_home.any() or need_away.any():
        name_map = build_name_maps()
        if not name_map.empty:
            df["pitcher_home_norm"] = df["pitcher_home"].map(normalize_name)
            df["pitcher_away_norm"] = df["pitcher_away"].map(normalize_name)
            nm = dict(zip(name_map["name_norm"], name_map["player_id"]))
            if need_home.any():
                df.loc[need_home, "pitcher_home_id"] = df.loc[need_home, "pitcher_home_norm"].map(nm)
            if need_away.any():
                df.loc[need_away, "pitcher_away_id"] = df.loc[need_away, "pitcher_away_norm"].map(nm)
            df = df.drop(columns=["pitcher_home_norm","pitcher_away_norm"], errors="ignore")

    # 3) Report any still-missing starters and fail fast
    unresolved = []
    if df["pitcher_home_id"].isna().any():
        for idx in df[df["pitcher_home_id"].isna()].index:
            unresolved.append({
                "game_id": df.at[idx, "game_id"],
                "team_side": "home",
                "team_id": df.at[idx, "home_team_id"],
                "pitcher_name": df.at[idx, "pitcher_home"]
            })
    if df["pitcher_away_id"].isna().any():
        for idx in df[df["pitcher_away_id"].isna()].index:
            unresolved.append({
                "game_id": df.at[idx, "game_id"],
                "team_side": "away",
                "team_id": df.at[idx, "away_team_id"],
                "pitcher_name": df.at[idx, "pitcher_away"]
            })

    if unresolved:
        miss_path = SUM_DIR / "missing_pitcher_ids.csv"
        pd.DataFrame(unresolved).sort_values(["game_id","team_side"]).to_csv(miss_path, index=False)
        print(f"❌ Unresolved pitcher IDs ({len(unresolved)}). See {miss_path}", file=sys.stderr)
        sys.exit(1)

    # 4) Success: write fixed schedule with Int64 IDs
    out = OUT_DIR / "todaysgames_normalized_fixed.csv"
    df.to_csv(out, index=False)
    print(f"✅ Fixed todaysgames_normalized written: {out}")

if __name__ == "__main__":
    main()
