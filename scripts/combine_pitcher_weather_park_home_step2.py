import pandas as pd
from pathlib import Path

def main():
    weather = pd.read_csv("data/adjusted/pitchers_home_weather.csv")
    park = pd.read_csv("data/adjusted/pitchers_home_park.csv")
    merged = pd.merge(weather, park[["name", "team", "adj_woba_park"]], on=["name", "team"], how="inner")
    merged["adj_woba_weather"] = merged["adj_woba_weather"].fillna(merged["woba"])
    merged["adj_woba_combined"] = (merged["adj_woba_weather"] + merged["adj_woba_park"]) / 2
    merged["adj_woba_combined"] = merged.apply(
        lambda r: r["adj_woba_park"] if pd.isna(r["adj_woba_weather"]) else r["adj_woba_combined"], axis=1)
    merged.to_csv("data/adjusted/pitchers_home_adjusted.csv", index=False)
    log_file = Path("data/adjusted/log_pitchers_combined_home.txt")
    top5 = merged[["name", "team", "adj_woba_weather", "adj_woba_park", "adj_woba_combined"]]         .sort_values(by="adj_woba_combined", ascending=False).head(5)
    with open(log_file, "w") as f:
        f.write("Top 5 home pitchers (combined adjustment):\n")
        f.write(top5.to_string(index=False))

if __name__ == "__main__":
    main()
