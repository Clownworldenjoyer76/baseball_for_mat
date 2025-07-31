
import pandas as pd
from pathlib import Path

def ensure_stats(file_path: Path, output_path: Path):
    df = pd.read_csv(file_path)

    for col in ["hr", "slg", "obp"]:
        if col not in df.columns:
            print(f"⚠️ Adding missing column: {col}")
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    ensure_stats(
        Path("data/end_chain/final/batter_home_final.csv"),
        Path("data/end_chain/final/batter_home_final.csv")
    )
    ensure_stats(
        Path("data/end_chain/final/batter_away_final.csv"),
        Path("data/end_chain/final/batter_away_final.csv")
    )
