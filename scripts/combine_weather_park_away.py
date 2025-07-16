import pandas as pd
from pathlib import Path
import subprocess

def load_data():
    print("📥 Loading input CSVs...")
    weather = pd.read_csv("data/adjusted/batters_away_weather.csv")
    park = pd.read_csv("data/adjusted/batters_away_park.csv")
    print(f"🔹 Weather rows: {len(weather)}")
    print(f"🔹 Park rows: {len(park)}")
    return weather, park

def merge_and_combine(weather, park):
    print("🔀 Merging weather and park adjustment files...")
    merged = pd.merge(
        weather,
        park[["last_name, first_name", "team", "adj_woba_park"]],
        on=["last_name, first_name", "team"],
        how="inner"
    )
    print(f"✅ Merged rows: {len(merged)}")
    print("📊 Columns in merged data:", merged.columns.tolist())

    if len(merged) == 0:
        print("⚠️ No rows matched between weather and park files. Check name/team formatting.")

    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2
    return merged

def save_output(df):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)

    output_file = out_path / "batters_away_adjusted.csv"
    df.to_csv(output_file, index=False)
    print(f"💾 Saved combined file to: {output_file}")

    log_file = out_path / "log_combined_away.txt"
    top5 = df[["last_name, first_name", "team", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]] \
        .sort_values(by="adj_woba_combined", ascending=False).head(5)

    with open(log_file, "w") as f:
        f.write("Top 5 away batters (combined adjustment):\n")
        f.write(top5.to_string(index=False))
    print(f"📝 Wrote top 5 log to: {log_file}")

def commit_outputs():
    print("📤 Committing and pushing results...")
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run([
            "git", "add",
            "data/adjusted/batters_away_adjusted.csv",
            "data/adjusted/log_combined_away.txt"
        ], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Auto-commit: Combined away batter adjustments"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit & push successful.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed: {e}")

def main():
    weather, park = load_data()
    combined = merge_and_combine(weather, park)
    save_output(combined)
    commit_outputs()

if __name__ == "__main__":
    main()
