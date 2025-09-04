#!/usr/bin/env python3
import pandas as pd
import unicodedata
import re
from pathlib import Path
from glob import glob

LINEUPS = Path("data/raw/lineups.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")
BATTERS = Path("data/Data/batters.csv")
PITCHERS = Path("data/Data/pitchers.csv")
TEAM_BATS_GLOB = "data/team_csvs/batters_*.csv"
TEAM_PITS_GLOB = "data/team_csvs/pitchers_*.csv"
REPORT = Path("summaries/todaysgames/lineups_fix_unmatched.txt")

# -------- helpers --------
def strip_accents(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFD", text)
    return "".join(c for c in text if unicodedata.category(c) != "Mn")

def normalize_name(name: str) -> str:
    """
    Normalize to 'Last [Suffix], First' with basic handling of Jr./Sr./II etc
    and keep multi-word last names like 'Del Castillo' intact.
    """
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = re.sub(r"[^a-zA-Z.,' ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    # if already in "Last, First" form, trust it (light clean)
    if "," in name:
        last, first = [x.strip() for x in name.split(",", 1)]
        return f"{last}, {first}"

    # From "First ... Last [Suffix?]" -> "Last [Suffix], First ..."
    suffixes = {"Jr", "Sr", "II", "III", "IV", "Jr.", "Sr."}
    tokens = name.split()
    if len(tokens) >= 2:
        last_parts = [tokens[-1]]
        # bring suffix with last name if present
        if tokens[-1].replace(".", "") in suffixes and len(tokens) >= 3:
            last_parts = [tokens[-2], tokens[-1]]
        first = " ".join(tokens[:-len(last_parts)])
        last = " ".join(last_parts)
        return f"{last.strip()}, {first.strip()}"

    return name.title()

def normalize_team_code(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", "", s.strip()).upper()

def explode_team_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a mapping of alias -> team_id from team_directory all_codes.
    Supports comma/pipe/slash/space separators.
    """
    out = {}
    for _, r in df.iterrows():
        tid = r.get("team_id")
        code = normalize_team_code(r.get("team_code", ""))
        if code:
            out[code] = tid
        aliases = r.get("all_codes", "")
        if isinstance(aliases, str) and aliases.strip():
            for a in re.split(r"[,\|/ ]+", aliases):
                a = normalize_team_code(a)
                if a:
                    out[a] = tid
    return pd.DataFrame(
        [{"code": k, "team_id": v} for k, v in out.items()]
    ).drop_duplicates(subset=["code"])

def load_master(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["last_name, first_name", "player_id"])
    df = pd.read_csv(path, dtype=str)
    # normalize the key
    key = "last_name, first_name"
    # some sources might use 'name'
    if key not in df.columns and "name" in df.columns:
        df = df.rename(columns={"name": key})
    if key not in df.columns or "player_id" not in df.columns:
        return pd.DataFrame(columns=[key, "player_id"])
    df[key] = df[key].map(normalize_name)
    df["player_id"] = df["player_id"].astype(str)
    return df[[key, "player_id"]].dropna().drop_duplicates()

def load_team_files(pattern: str) -> pd.DataFrame:
    frames = []
    for p in glob(pattern):
        try:
            tdf = pd.read_csv(p, dtype=str)
            if "last_name, first_name" in tdf.columns and "player_id" in tdf.columns:
                tdf["last_name, first_name"] = tdf["last_name, first_name"].map(normalize_name)
                tdf["player_id"] = tdf["player_id"].astype(str)
                frames.append(tdf[["last_name, first_name", "player_id"]])
        except Exception:
            pass
    if frames:
        return pd.concat(frames, ignore_index=True).drop_duplicates()
    return pd.DataFrame(columns=["last_name, first_name", "player_id"])

# -------- main --------
def main():
    LINEUPS.parent.mkdir(parents=True, exist_ok=True)
    REPORT.parent.mkdir(parents=True, exist_ok=True)

    # Load and normalize lineups
    df = pd.read_csv(LINEUPS, dtype=str).fillna("")
    # ensure required cols exist
    for col in ["team_code", "last_name, first_name"]:
        if col not in df.columns:
            df[col] = ""
    df["team_code"] = df["team_code"].map(normalize_team_code)
    df["last_name, first_name"] = df["last_name, first_name"].map(normalize_name)

    # Ensure output columns exist
    for col in ["type", "player_id", "team_id"]:
        if col not in df.columns:
            df[col] = ""

    # Load master sources
    bat = load_master(BATTERS)
    pit = load_master(PITCHERS)
    bat_team = load_team_files(TEAM_BATS_GLOB)
    pit_team = load_team_files(TEAM_PITS_GLOB)

    # Build name->player_id maps (priority order)
    # 1) master batters, 2) master pitchers, 3) team batters, 4) team pitchers
    name_to_pid = {}
    for source in (bat, pit, bat_team, pit_team):
        for _, r in source.iterrows():
            nm = r["last_name, first_name"]
            pid = r["player_id"]
            if nm and pid and nm not in name_to_pid:
                name_to_pid[nm] = pid

    # Fill player_id
    df["player_id"] = df.apply(
        lambda r: name_to_pid.get(r["last_name, first_name"], r["player_id"]),
        axis=1
    )

    # Fill type based on where name matched
    bat_names = set(bat["last_name, first_name"].tolist()) | set(bat_team["last_name, first_name"].tolist())
    pit_names = set(pit["last_name, first_name"].tolist()) | set(pit_team["last_name, first_name"].tolist())

    def infer_type(r):
        nm = r["last_name, first_name"]
        if nm in bat_names:
            return "batter"
        if nm in pit_names:
            return "pitcher"
        return r["type"]

    df["type"] = df.apply(infer_type, axis=1)

    # Team ID mapping
    if TEAM_DIR.exists():
        td = pd.read_csv(TEAM_DIR, dtype=str).fillna("")
        # normalize incoming directory columns
        if "team_code" in td.columns:
            td["team_code"] = td["team_code"].map(normalize_team_code)
        if "team_id" in td.columns:
            td["team_id"] = td["team_id"].astype(str)

        # direct code match
        direct = td[["team_code", "team_id"]].drop_duplicates()
        code_map = dict(zip(direct["team_code"], direct["team_id"]))

        # alias map
        alias_df = explode_team_aliases(td)
        alias_map = dict(zip(alias_df["code"], alias_df["team_id"]))

        def map_team_id(code: str, current: str) -> str:
            if current:  # keep existing if present
                return current
            if code in code_map:
                return code_map[code]
            if code in alias_map:
                return alias_map[code]
            return ""

        df["team_id"] = df.apply(lambda r: map_team_id(r["team_code"], r["team_id"]), axis=1)

    # Save and report
    df.to_csv(LINEUPS, index=False)

    # Debug: who didn’t match?
    missing_pid = df[df["player_id"].astype(str).eq("")][["team_code", "last_name, first_name"]]
    missing_tid = df[df["team_id"].astype(str).eq("")][["team_code", "last_name, first_name"]]
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("Missing player_id:\n")
        if len(missing_pid):
            f.write(missing_pid.to_csv(index=False))
        else:
            f.write("None\n")
        f.write("\nMissing team_id:\n")
        if len(missing_tid):
            f.write(missing_tid.to_csv(index=False))
        else:
            f.write("None\n")

    print(f"✅ lineups_fix.py: wrote {len(df)} rows to {LINEUPS}")
    print(f"📝 unmatched report: {REPORT}")

if __name__ == "__main__":
    main()
