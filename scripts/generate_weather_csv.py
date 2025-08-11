import pandas as pd
from pathlib import Path
import subprocess
import os
import re
from unidecode import unidecode

# --- File Paths ---
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def _norm(s: str) -> str:
    """Normalize strings for matching: ASCII, lower, remove non-letters."""
    s = unidecode(str(s or "")).strip().lower()
    return re.sub(r"[^a-z]", "", s)

def _build_team_alias_map(team_map_df: pd.DataFrame) -> dict:
    """
    Build many-to-one alias map -> canonical nickname (team_name or clean_team_name).
    Accepts columns: team_code, abbreviation, team_name, clean_team_name.
    """
    alias_to_canonical = {}
    for _, r in team_map_df.iterrows():
        team_name = (r.get("team_name") or r.get("clean_team_name") or "").strip()
        clean_name = (r.get("clean_team_name") or team_name or "").strip()
        code = (r.get("team_code") or "").strip()
        abbr = (r.get("abbreviation") or "").strip()

        if not team_name and not clean_name:
            continue

        canonical = team_name if team_name else clean_name

        variants = set()
        if team_name:
            variants.add(_norm(team_name))
        if clean_name:
            variants.add(_norm(clean_name))
        if code:
            variants.add(_norm(code))
        if abbr:
            variants.add(_norm(abbr))

        for v in variants:
            if v:
                alias_to_canonical[v] = canonical

    return alias_to_canonical

def _resolve_team(raw: str, alias_map: dict, canonical_choices: set) -> str:
    """
    Resolve an incoming team label to canonical nickname using:
      1) direct alias map,
      2) suffix match vs canonical nicknames (handles City+Nickname),
      3) last-2-words / last-word heuristics (fixes 'White Sox', 'Red Sox', etc.).
    """
    s = (raw or "").strip()
    n = _norm(s)
    if not n:
        return ""

    # 1) direct alias hit
    if n in alias_map:
        return alias_map[n]

    # 2) suffix match against canonical nicknames
    for canon in canonical_choices:
        if n.endswith(_norm(canon)):
            return canon

    # 3) last-2-words or last-word heuristic
    tokens = re.findall(r"[A-Za-z]+", unidecode(s))
    if tokens:
        if len(tokens) >= 2:
            last2 = " ".join(tokens[-2:])
            for cand in (last2, tokens[-1]):
                cand_n = _norm(cand)
                if cand_n in alias_map:
                    return alias_map[cand_n]
                for canon in canonical_choices:
                    if cand_n == _norm(canon):
                        return canon
        else:
            cand_n = _norm(tokens[-1])
            if cand_n in alias_map:
                return alias_map[cand_n]
            for canon in canonical_choices:
                if cand_n == _norm(canon):
                    return canon

    return ""

def generate_weather_csv():
    try:
        games_df = pd.read_csv(GAMES_FILE)
        stadium_df = pd.read_csv(STADIUM_FILE)
        team_map_df = pd.read_csv(TEAM_MAP_FILE)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return

    # Normalize column names
    games_df.columns = [c.strip() for c in games_df.columns]
    stadium_df.columns = [c.strip() for c in stadium_df.columns]
    team_map_df.columns = [c.strip() for c in team_map_df.columns]

    # Build robust alias map
    alias_map = _build_team_alias_map(team_map_df)

    # Canonical set of nicknames we will keep in output
    # FIX: replace string Series "|" with combine_first
    team_name_series = team_map_df.get("team_name", pd.Series(dtype=str)).astype(str).str.strip().replace({"": pd.NA})
    clean_name_series = team_map_df.get("clean_team_name", pd.Series(dtype=str)).astype(str).str.strip().replace({"": pd.NA})
    canonical_series = team_name_series.combine_first(clean_name_series)
    canonical_choices = set(canonical_series.dropna().tolist())

    # --- Resolve incoming home/away from games_df to canonical nicknames ---
    if "home_team" not in games_df.columns or "away_team" not in games_df.columns:
        print("❌ games file must include 'home_team' and 'away_team'.")
        return

    games_df["home_team_resolved"] = games_df["home_team"].apply(
        lambda x: _resolve_team(x, alias_map, canonical_choices)
    )
    games_df["away_team_resolved"] = games_df["away_team"].apply(
        lambda x: _resolve_team(x, alias_map, canonical_choices)
    )

    # Warn on unresolved teams (prevents silent blanks)
    unresolved_home = games_df[games_df["home_team_resolved"] == ""]
    unresolved_away = games_df[games_df["away_team_resolved"] == ""]
    if not unresolved_home.empty or not unresolved_away.empty:
        print("⚠️ Unresolved team names detected:")
        if not unresolved_home.empty:
            print("  Home unresolved:", unresolved_home[["home_team"]].drop_duplicates().to_dict(orient="list"))
        if not unresolved_away.empty:
            print("  Away unresolved:", unresolved_away[["away_team"]].drop_duplicates().to_dict(orient="list"))

    # --- Prepare stadium_df for merge (uses nicknames in examples) ---
    stadium_df["home_team_stadium"] = stadium_df["home_team"].astype(str).str.strip()

    # Drop 'game_time' from games (we keep stadium's game_time)
    games_df.drop(columns=["game_time"], errors="ignore", inplace=True)

    # --- Merge with Stadium Info on canonical home nickname ---
    merged = pd.merge(
        games_df,
        stadium_df.drop(columns=["home_team"], errors="ignore"),
        left_on="home_team_resolved",
        right_on="home_team_stadium",
        how="left"
    )

    if merged.empty:
        print("❌ Merge failed: No matching rows after games + stadium merge.")
        return

    merged.drop(columns=["home_team_stadium"], inplace=True, errors="ignore")

    # --- Set final canonical home/away team columns ---
    merged.rename(columns={"home_team_resolved": "home_team",
                           "away_team_resolved": "away_team"}, inplace=True)

    # --- Drop merge artifacts if any exist ---
    for col in ["away_team_x", "away_team_y", "team_name_original", "team_name_mapped", "uppercase"]:
        if col in merged.columns:
            merged.drop(columns=[col], inplace=True)

    # --- Reorder columns for clarity (only those that exist) ---
    preferred_order = [
        "home_team", "away_team", "pitcher_home", "pitcher_away",
        "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude",
        "game_time", "time_of_day", "Park Factor"
    ]
    merged = merged[[c for c in preferred_order if c in merged.columns]]

    # --- Validate empties after mapping ---
    required_nonempty = ["home_team", "away_team", "venue", "city", "latitude", "longitude", "game_time"]
    empties = []
    for c in required_nonempty:
        if c in merged.columns and merged[c].isna().any():
            empties.append(c)
    if empties:
        print(f"⚠️ Warning: missing values detected in: {', '.join(empties)}")
        print(merged[merged[empties].isnull().any(axis=1)])

    # --- Row count check ---
    try:
        expected = len(games_df)
        got = len(merged)
        if expected != got:
            print(f"⚠️ Row mismatch: expected {expected}, got {got}")
    except Exception:
        pass

    # --- Output to CSV ---
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    summary = (
        f"✅ Weather input file generated\n"
        f"🔢 Rows: {len(merged)}\n"
        f"📁 Output: {OUTPUT_FILE}\n"
        f"📄 Games file: {GAMES_FILE}\n"
        f"🏟️ Stadium file: {STADIUM_FILE}"
    )
    print(summary)
    Path(SUMMARY_FILE).write_text(summary)

    # --- Git Operations ---
    try:
        git_env = os.environ.copy()
        git_env["GIT_AUTHOR_NAME"] = "github-actions"
        git_env["GIT_AUTHOR_EMAIL"] = "github-actions@github.com"

        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True, env=git_env)

        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True, env=git_env).stdout
        if not status_output.strip():
            print("✅ No changes to commit.")
        else:
            subprocess.run(["git", "commit", "-m", "🔁 Update data files and weather input/summary"], check=True, capture_output=True, text=True, env=git_env)
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True, env=git_env)
            print("✅ Git commit and push complete.")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed:")
        print(f"  Command: {e.cmd}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT: {e.stdout}")
        print(f"  STDERR: {e.stderr}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations: {e}")

if __name__ == "__main__":
    generate_weather_csv()
