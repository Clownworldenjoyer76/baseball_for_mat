import pandas as pd
from pathlib import Path
import subprocess

def load_data():
    print("📥 Loading input CSVs...")
    weather = pd.read_csv("data/adjusted/batters_home_weather.csv")
    park = pd.read_csv("data/adjusted/batters_home_park.csv")
    print(f"🔹 Weather rows: {len(weather)}")
    print(f"🔹 Park rows: {len(park)}")
    return weather, park

def merge_and_combine(weather, park):
    print("🔀 Merging weather and park adjustment files...")

    # Only keep batters
    weather = weather[weather["type"] == "batter"]
    park = park[park["type"] == "batter"]

    if "player_id" not in weather.columns or "player_id" not in park.columns:
        raise ValueError("player_id column missing in one of the input files")

    merged = pd.merge(
        weather,
        park[["player_id", "adj_woba_park"]],
        on="player_id",
        how="inner"
    )
    print(f"✅ Merged rows: {len(merged)}")
    if len(merged) == 0:
        print("⚠️ No rows matched. Check player_id column contents.")
    if merged.duplicated(subset=["player_id"]).any():
        print("⚠️ Warning: Duplicate player_id rows detected in merged result.")

    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2
    return merged

def save_output(df):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)

    output_file = out_path / "batters_home_adjusted.csv"
    df.to_csv(output_file, index=False)
    print(f"💾 Saved to: {output_file}")

    log_file = out_path / "log_combined_home.txt"
    top5 = df[["player_id", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]] \
        .sort_values(by="adj_woba_combined", ascending=False).head(5)

    with open(log_file, "w") as f:
        f.write("Top 5 home batters (combined adjustment):\n")
        f.write(top5.to_string(index=False))
    print(f"📝 Log written to: {log_file}")

def commit_outputs():
    print("📤 Committing and pushing to repo...")
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run([
            "git", "add",
            "data/adjusted/batters_home_adjusted.csv",
            "data/adjusted/log_combined_home.txt"
        ], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Auto-commit: Combined home batter adjustments"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Git commit and push successful.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")

def main():
    weather, park = load_data()
    combined = merge_and_combine(weather, park)
    save_output(combined)
    commit_outputs()

if __name__ == "__main__":
    main()
