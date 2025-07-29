
import pandas as pd
from pathlib import Path

# === Config ===
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
BAT_TODAY_FILE = Path("data/raw/bat_today_normalized.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")

OUTPUT_DIR = Path("data/end_chain/final/")


# === Input file paths ===
BAT_HOME_FILE = INPUT_DIR / "bat_home3csv"
BAT_AWAY_FILE = INPUT_DIR / "bat_away4.csv"
BAT_TODAY_FILE = INPUT_DIR / "bat_today.csv"
PITCHERS_FILE = INPUT_DIR / "startingpitchers.csv"

# === Output file paths ===
BAT_HOME_FINAL = OUTPUT_DIR / "batter_home_final.csv"
BAT_AWAY_FINAL = OUTPUT_DIR / "batter_away_final.csv"
BAT_TODAY_FINAL = OUTPUT_DIR / "bat_today_final.csv"
PITCHERS_FINAL = OUTPUT_DIR / "startingpitchers_final.csv"

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    return pd.read_csv(path)

def enforce_last_first(name):
    if not isinstance(name, str) or "," not in name:
        parts = name.strip().split()
        if len(parts) >= 2:
            return f"{parts[-1].capitalize()}, {' '.join(p.capitalize() for p in parts[:-1])}"
        return name
    return name.strip()

def main():
    print("🔄 Loading normalized files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    bat_today = load_csv(BAT_TODAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)

    print("✅ Files loaded. Checking formats...")

    # Enforce final name formatting again before final save
    bat_home["pitcher_home"] = bat_home["pitcher_home"].apply(enforce_last_first)
    bat_away["pitcher_away"] = bat_away["pitcher_away"].apply(enforce_last_first)
    bat_today["name"] = bat_today["name"].apply(enforce_last_first)
    bat_home["last_name, first_name"] = bat_home["last_name, first_name"].apply(enforce_last_first)
    bat_away["last_name, first_name"] = bat_away["last_name, first_name"].apply(enforce_last_first)
    bat_today["last_name, first_name"] = bat_today["last_name, first_name"].apply(enforce_last_first)
    pitchers["last_name, first_name"] = pitchers["last_name, first_name"].apply(enforce_last_first)

    print("💾 Saving renamed final files...")
    bat_home.to_csv(BAT_HOME_FINAL, index=False)
    bat_away.to_csv(BAT_AWAY_FINAL, index=False)
    bat_today.to_csv(BAT_TODAY_FINAL, index=False)
    pitchers.to_csv(PITCHERS_FINAL, index=False)

    print("✅ Final files saved to normalize_end.")

if __name__ == "__main__":
    main()
