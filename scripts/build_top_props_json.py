import pandas as pd
import json
from pathlib import Path

INPUT_FILE = "data/final/prop_candidates.csv"
OUTPUT_FILE = "data/final/top_props.json"

def main():
    df = pd.read_csv(INPUT_FILE)

    if df.empty:
        print("❌ prop_candidates.csv is empty.")
        return

    # Sort by wOBA descending (higher is better)
    if "adj_woba_combined" not in df.columns:
        print("❌ Missing adj_woba_combined column.")
        return

    props = df.sort_values(by="adj_woba_combined", ascending=False).head(5)

    result = []
    for _, row in props.iterrows():
        result.append({
            "name": row.get("name", ""),
            "team": row.get("team", ""),
            "stat": "adj_woba_combined",
            "score": round(row.get("adj_woba_combined", 0), 2)
        })

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Saved top props to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
