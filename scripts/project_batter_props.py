import pandas as pd
from pathlib import Path
from projection_formulas import project_batter_props

# File paths
BAT_HOME_FILE = Path("data/end_chain/final/batter_home_final.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/batter_away_final.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
FALLBACK_FILE = Path("data/end_chain/final/bat_today_final.csv")
OUTPUT_FILE = Path("data/end_chain/complete/batter_props_projected.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    print("🔄 Loading input files...")
    df_home = load_csv(BAT_HOME_FILE)
    df_away = load_csv(BAT_AWAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    fallback = load_csv(FALLBACK_FILE)

    print("🧠 Projecting batter props...")
    projected_home = project_batter_props(df_home, pitchers, "home", fallback)
    projected_away = project_batter_props(df_away, pitchers, "away", fallback)

    combined = pd.concat([projected_home, projected_away], ignore_index=True)

    print(f"💾 Saving results to {OUTPUT_FILE}")
    combined.to_csv(OUTPUT_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()