#!/usr/bin/env python3
"""
Backfill missing pitcher IDs in data/raw/todaysgames_normalized.csv
by normalized name, using multiple in-repo sources in this priority:
  1) data/processed/player_team_master.csv
  2) data/Data/pitchers.csv
  3) data/raw/startingpitchers_with_opp_context.csv   <-- new authoritative fallback per game

On success:
  - writes data/_projections/todaysgames_normalized_fixed.csv (IDs as integers)

On failure:
  - writes summaries/projections/missing_pitcher_ids.csv and exits 1
  - may write summaries/projections/missing_master_columns.txt if inputs lack name/ID columns
"""

from pathlib import Path
import sys
import pandas as pd
import unicodedata

RAW_TODAY   = Path("data/raw/todaysgames_normalized.csv")
MASTER      = Path("data/processed/player_team_master.csv")      # preferred map
SEASON_P    = Path("data/Data/pitchers.csv")                     # fallback map
STARTERS_26 = Path("data/raw/startingpitchers_with_opp_context.csv")  # per-run authoritative list (26 rows)
SUM_DIR     = Path("summaries/projections")
OUT_DIR     = Path("data/_projections")

SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ------------ helpers ------------

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    # NFKD accent strip
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def normalize_name(val) -> str:
    """
    Lowercase, strip accents/punct, collapse spaces, special-case particles.
    """
    if not isinstance(val, str):
        return ""
    s = val.strip()
    s = _strip_accents(s).lower()
    for ch in [".", ",", "'", "\"", "’", "`", "´", "-", "–", "—", "(", ")", "/"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    # Special cases for common particles/concat names
    s = s.replace(" de grom", " degrom")
    s = s.replace(" de la ", " dela ")
    s = s.replace(" o' ", " o ")
    return s

def find_id_col(df: pd.DataFrame) -> str | None:
    for c in ["player_id", "mlb_id", "person_id", "id", "retro_id", "bbref_id"]:
        if c in df.columns:
            return c
    return None

def find_single_name_col(df: pd.DataFrame) -> str | None:
    for c in ["player_name", "name", "full_name", "mlb_name", "display_name"]:
        if c in df.columns:
            return c
    return None

def build_name_norm_column(df: pd.DataFrame, *, new_col: str = "name_norm") -> bool:
    if df.empty:
        return False
    single = find_single_name_col(df)
    if single:
        df[new_col] = df[single].map(normalize_name)
        return True
    # try first/last
    first_opts = ["first_name", "firstname", "given_name"]
    last_opts  = ["last_name", "lastname", "family_name", "surname"]
    first = next((c for c in first_opts if c in df.columns), None)
    last  = next((c for c in last_opts  if c in df.columns), None)
    if first and last:
        df[new_col] = (df[first].fillna("") + " " + df[last].fillna("")).map(normalize_name)
        return True
    return False

def write_text(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8")

def to_intlike(series: pd.Series) -> pd.Series:
    """
    Convert float/str IDs like '661563.0' -> Int64(661563). Keep NA as <NA>.
    """
    out = pd.to_numeric(series, errors="coerce").astype("Int64")
    return out

# ------------ main ------------

def main() -> None:
    # Load today's schedule
    if not RAW_TODAY.exists():
        write_text(SUM_DIR / "missing_master_columns.txt", f"Missing input file: {RAW_TODAY}")
        print(f"❌ Missing input: {RAW_TODAY}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(RAW_TODAY)

    # Validate required schedule columns
    required_sched_cols = [
        "game_id",
        "home_team_id", "away_team_id",
        "pitcher_home", "pitcher_away",
        "pitcher_home_id", "pitcher_away_id",
    ]
    missing_sched = [c for c in required_sched_cols if c not in df.columns]
    if missing_sched:
        write_text(SUM_DIR / "missing_master_columns.txt", f"todaysgames_normalized.csv missing columns: {missing_sched}")
        print(f"❌ Schedule missing columns: {missing_sched}", file=sys.stderr)
        sys.exit(1)

    # Normalize schedule names; ensure ID cols are Int64 for reliable NA detection
    df["pitcher_home_norm"] = df["pitcher_home"].map(normalize_name)
    df["pitcher_away_norm"] = df["pitcher_away"].map(normalize_name)
    df["pitcher_home_id"] = to_intlike(df["pitcher_home_id"])
    df["pitcher_away_id"] = to_intlike(df["pitcher_away_id"])

    # Build consolidated name->id map from multiple sources
    maps = []

    # 1) Master
    if MASTER.exists():
        master = pd.read_csv(MASTER)
        ok_name = build_name_norm_column(master, new_col="name_norm")
        id_col = find_id_col(master)
        if ok_name and id_col:
            tmp = master[["name_norm", id_col]].dropna()
            tmp = tmp.rename(columns={id_col: "player_id"})
            tmp["player_id"] = to_intlike(tmp["player_id"])
            maps.append(tmp[["name_norm", "player_id"]].dropna().drop_duplicates())
        else:
            msg = []
            if not ok_name: msg.append("player_team_master.csv: no recognizable name column.")
            if not id_col:  msg.append("player_team_master.csv: no recognizable player_id column.")
            if msg:
                prev = (SUM_DIR / "missing_master_columns.txt").read_text("utf-8") if (SUM_DIR / "missing_master_columns.txt").exists() else ""
                write_text(SUM_DIR / "missing_master_columns.txt", (prev + ("\n" if prev else "") + " | ".join(msg)))

    # 2) Season pitchers
    if SEASON_P.exists():
        season = pd.read_csv(SEASON_P)
        ok_name = build_name_norm_column(season, new_col="name_norm")
        id_col = find_id_col(season)
        if ok_name and id_col:
            tmp = season[["name_norm", id_col]].dropna()
            tmp = tmp.rename(columns={id_col: "player_id"})
            tmp["player_id"] = to_intlike(tmp["player_id"])
            maps.append(tmp[["name_norm", "player_id"]].dropna().drop_duplicates())
        else:
            msg = []
            if not ok_name: msg.append("pitchers.csv: no recognizable name column.")
            if not id_col:  msg.append("pitchers.csv: no recognizable player_id column.")
            if msg:
                prev = (SUM_DIR / "missing_master_columns.txt").read_text("utf-8") if (SUM_DIR / "missing_master_columns.txt").exists() else ""
                write_text(SUM_DIR / "missing_master_columns.txt", (prev + ("\n" if prev else "") + " | ".join(msg)))

    # 3) Starting pitchers (26 rows) for authoritative per-game mapping
    if STARTERS_26.exists():
        sp = pd.read_csv(STARTERS_26)
        # try to identify name/ID columns on both sides
        # expected columns include: game_id, starter_name, player_id OR separate home/away; handle flexibly
        name_cols = [c for c in sp.columns if "name" in c.lower() or "pitcher" in c.lower()]
        id_cols   = [c for c in sp.columns if "player_id" in c.lower() or c.lower().endswith("_id")]
        # Build pairs by scanning rows
        rows = []
        for _, r in sp.iterrows():
            for c in name_cols:
                nm = r.get(c, None)
                if not isinstance(nm, str) or not nm.strip():
                    continue
                nm_norm = normalize_name(nm)
                # find any id-like value on same row
                pid = None
                # prefer fields that end with '_id' or contain 'player_id'
                for ic in id_cols:
                    val = r.get(ic, None)
                    if pd.notna(val):
                        pid = val
                        break
                if pid is not None:
                    rows.append({"name_norm": nm_norm, "player_id": pid})
        if rows:
            tmp = pd.DataFrame(rows)
            tmp["player_id"] = to_intlike(tmp["player_id"])
            maps.append(tmp.dropna().drop_duplicates())

    # Merge maps (later maps can fill holes but we’ll do deterministic selection below)
    if maps:
        map_df = pd.concat(maps, ignore_index=True).dropna(subset=["name_norm", "player_id"]).drop_duplicates()
    else:
        map_df = pd.DataFrame(columns=["name_norm", "player_id"])

    # function to resolve one side
    unresolved = []

    def resolve(side: str):
        pid_col  = f"pitcher_{side}_id"
        norm_col = f"pitcher_{side}_norm"

        need = df[pid_col].isna()
        if not need.any():
            return

        # vector map by name
        name_to_id = dict(zip(map_df["name_norm"], map_df["player_id"]))
        filled = df[norm_col].map(name_to_id)
        # fill only where missing
        df.loc[need, pid_col] = filled[need]

        # gather unresolved
        still = df[pid_col].isna()
        for idx in df[still].index:
            unresolved.append({
                "game_id":      df.at[idx, "game_id"],
                "team_side":    side,
                "team_id":      df.at[idx, f"{side}_team_id"],
                "pitcher_name": df.at[idx, f"pitcher_{side}"]
            })

    resolve("home")
    resolve("away")

    if unresolved:
        miss_df = pd.DataFrame(unresolved).sort_values(["game_id", "team_side"])
        miss_path = SUM_DIR / "missing_pitcher_ids.csv"
        miss_df.to_csv(miss_path, index=False)
        print(f"❌ Unresolved pitcher IDs ({len(miss_df)}). See {miss_path}", file=sys.stderr)
        sys.exit(1)

    # Success: drop helper cols, write fixed output with integer IDs
    df = df.drop(columns=["pitcher_home_norm", "pitcher_away_norm"])
    # Ensure IDs are ints in output CSV
    df["pitcher_home_id"] = df["pitcher_home_id"].astype("Int64")
    df["pitcher_away_id"] = df["pitcher_away_id"].astype("Int64")
    out_path = OUT_DIR / "todaysgames_normalized_fixed.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Fixed todaysgames_normalized written: {out_path}")

if __name__ == "__main__":
    main()
