import pandas as pd
from pathlib 
from unidecode import unidecode

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name)
    name = name.lower().strip()
    name = ' '.join(name.split())
    name = ','.join(part.strip() for part in name.split(','))
    return name.title()

import Path
import subprocess

def load_data():
    weather = pd.read_csv("data/adjusted/pitchers_away_weather.csv")
    park = pd.read_csv("data/adjusted/pitchers_away_park.csv")
    return weather, park

def merge_and_combine(weather, park):
    merged = pd.merge(
        weather,
        park[["name", "team", "adj_woba_park"]],
        on=["name", "team"],
        how="inner"
    )
    merged["adj_woba_weather"] = merged["adj_woba_weather"].fillna(merged["woba"])
    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2
    merged["adj_woba_combined"] = merged.apply(
        lambda row: row["adj_woba_park"] if pd.isna(row["adj_woba_weather"]) else row["adj_woba_combined"],
        axis=1
    )
    return merged

def save_output(df):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    output_file = out_path / "pitchers_away_weather_park.csv"  # ✅ updated file name
    df["name"] = df["name"].apply(normalize_name)
df.to_csv(output_file, index=False)

    log_file = out_path / "log_pitchers_combined_away.txt"
    top5 = df[["name", "team", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]]         .sort_values(by="adj_woba_combined", ascending=False).head(5)
    with open(log_file, "w") as f:
        f.write("Top 5 away pitchers (combined adjustment):\n")
        f.write(top5.to_string(index=False))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run([
            "git", "add",
            "data/adjusted/pitchers_away_weather_park.csv",
            "data/adjusted/log_pitchers_combined_away.txt"
        ], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Auto-commit: Combined away pitcher adjustments"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Forced commit and push to repo.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    weather, park = load_data()
    combined = merge_and_combine(weather, park)
    print("✅ Merged rows:", len(combined))
    save_output(combined)
    commit_outputs()

if __name__ == "__main__":
    main()
