import pandas as pd
from pathlib import Path

def load_data():
    weather = pd.read_csv("data/adjusted/batters_away_weather.csv")
    park = pd.read_csv("data/adjusted/batters_away_park.csv")
    return weather, park

def merge_and_combine(weather, park):
    merged = pd.merge(
        weather,
        park[["last_name, first_name", "team", "adj_woba_park"]],
        on=["last_name, first_name", "team"],
        how="inner"
    )

    # Calculate combined wOBA (simple average; adjust weight if needed)
    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2

    return merged

def save_output(df):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    output_file = out_path / "batters_away_adjusted.csv"
    df.to_csv(output_file, index=False)

    # Optional: log top 5
    log_file = out_path / "log_combined_away.txt"
    top5 = df[["last_name, first_name", "team", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]] \
        .sort_values(by="adj_woba_combined", ascending=False).head(5)
    with open(log_file, "w") as f:
        f.write("Top 5 away batters (combined adjustment):\n")
        f.write(top5.to_string(index=False))

def main():
    weather, park = load_data()
    combined = merge_and_combine(weather, park)
    print("✅ Merged rows:", len(combined))
    save_output(combined)

if __name__ == "__main__":
    main()
