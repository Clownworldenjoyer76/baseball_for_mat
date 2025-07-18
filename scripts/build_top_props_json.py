import pandas as pd
import json
import subprocess

INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/top_props.json"

def main():
    df = pd.read_csv(INPUT_FILE)
    props = df[df["type"] == "prop"].copy()

    if props.empty:
        print("❌ No props found in input.")
        return

    top_props = props.sort_values(by="score", ascending=False).head(5)

    result = []
    for _, row in top_props.iterrows():
        result.append({
            "name": row.get("name", ""),
            "team": row.get("team", ""),
            "stat": row.get("stat", ""),
            "score": round(row.get("score", 0), 2)
        })

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Saved top props to {OUTPUT_FILE}")

    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Add top props JSON output"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
