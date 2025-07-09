import os
import pandas as pd

files = {
    "batters": "data/tagged/batters_normalized.csv",
    "pitchers": "data/tagged/pitchers_normalized.csv"
}
output_dir = "data/cleaned"
os.makedirs(output_dir, exist_ok=True)

for label, path in files.items():
    if os.path.exists(path):
        df = pd.read_csv(path)
        before = len(df)
        df = df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
        after = len(df)
        df.to_csv(f"{output_dir}/{label}_normalized_cleaned.csv", index=False)
        print(f"🧼 {label.capitalize()} deduplicated: {before} → {after}")
