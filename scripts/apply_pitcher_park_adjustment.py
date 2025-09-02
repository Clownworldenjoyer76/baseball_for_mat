# scripts/apply_pitcher_park_adjustment.py

import pandas as pd
from pathlib import Path
import subprocess
import csv

# Inputs
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"

# Outputs
OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_park.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_park.csv"
LOG_HOME = "log_pitchers_home_park.txt"
LOG_AWAY = "log_pitchers_away_park.txt"

# Stats to scale by park factor (percent-like; data provides Park Factor such as 101, 98, etc.)
STATS_TO_ADJUST = [
    "home_run",
    "slg_percent",
    "xslg",
    "woba",
    "xwoba",
    "barrel_batted_rate",
    "hard_hit_percent",
]

def load_games(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"game_id", "park_factor"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {path}: {sorted(missing)}")
    # Keep only what we need and ensure correct types
    out = df[["game_id", "park_factor"]].copy()
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["park_factor"] = pd.to_numeric(out["park_factor"], errors="coerce")
    return out.dropna(subset=["game_id", "park_factor"])

def load_pitchers(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "game_id" not in df.columns:
        raise ValueError(f"Missing 'game_id' in {path}")
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    return df.dropna(subset=["game_id"])

def apply_park(df_pitchers: pd.DataFrame, df_games: pd.DataFrame, side: str):
    log = []
    # Merge strictly on game_id (no team/name matching)
    merged = df_pitchers.merge(df_games, on="game_id", how="left", validate="m:1")
    missing_pf = merged["park_factor"].isna().sum()
    if missing_pf:
        log.append(f"⚠️ {missing_pf} rows missing park_factor after merge on game_id")

    # Scale stats by park_factor/100
    scale = merged["park_factor"] / 100.0
    for col in STATS_TO_ADJUST:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")
            merged[col] = merged[col] * scale
        else:
            log.append(f"ℹ️ '{col}' not present in input — skipped")

    # Provide explicit adjusted wOBA from (possibly scaled) woba
    if "woba" in merged.columns:
        merged["adj_woba_park"] = merged["woba"]
    else:
        merged["adj_woba_park"] = pd.NA
        log.append("❌ 'woba' missing — 'adj_woba_park' set to NA")

    # Top-5 log by adj_woba_park (if available)
    try:
        if merged["adj_woba_park"].notna().any():
            top5_cols = [c for c in ["name", "team", "adj_woba_park", "game_id", "park_factor"] if c in merged.columns]
            top5 = merged.sort_values("adj_woba_park", ascending=False).head(5)
            log.append("Top 5 by adj_woba_park:")
            log.append(top5[top5_cols].to_string(index=False))
        else:
            log.append("ℹ️ No non-NA 'adj_woba_park' values to rank")
    except Exception as e:
        log.append(f"⚠️ Failed to build Top 5: {e}")

    return merged, log

def save_output(df: pd.DataFrame, log_lines, file_path: str, log_path: str):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False, quoting=csv.QUOTE_MINIMAL)
    with open(log_path, "w") as f:
        f.write("\n".join(log_lines))
    print(f"✅ Wrote {file_path} ({len(df)} rows)")
    print(f"📝 Log written to {log_path}")

def git_commit_and_push():
    try:
        subprocess.run(["git", "add", "data/adjusted/pitchers_*_park.csv", "log_pitchers_*_park.txt"], check=True)
        status = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if status.strip():
            subprocess.run(["git", "commit", "-m", "Apply pitcher park adjustments (game_id merge only)"], check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ Git commit and push complete for pitcher park adjustments")
        else:
            print("✅ No changes to commit")
    except Exception as e:
        print(f"⚠️ Git operation failed: {e}")

def main():
    try:
        games = load_games(GAMES_FILE)
        home = load_pitchers(PITCHERS_HOME_FILE)
        away = load_pitchers(PITCHERS_AWAY_FILE)
    except Exception as e:
        print(f"❌ Load error: {e}")
        return

    adj_home, log_home = apply_park(home, games, "home")
    adj_away, log_away = apply_park(away, games, "away")

    save_output(adj_home, log_home, OUTPUT_HOME_FILE, LOG_HOME)
    save_output(adj_away, log_away, OUTPUT_AWAY_FILE, LOG_AWAY)

    git_commit_and_push()

if __name__ == "__main__":
    main()
