import pandas as pd
from pathlib import Path
import subprocess

def load_weather_and_games():
    weather = pd.read_csv("data/weather_adjustments.csv")
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    games["hour"] = pd.to_datetime(games["game_time"], format="%I:%M %p").dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if x < 18 else "night")
    return weather, games

def load_park_factors(time_of_day):
    file = f"data/Data/park_factors_{time_of_day}.csv"
    return pd.read_csv(file)[["home_team", "Park Factor"]]

def apply_weather_adjustment(pitchers, weather, games, label):
    if label == "home":
        pitchers["home_team"] = pitchers["team"]
    else:
        away_to_home = games.set_index("away_team")["home_team"].to_dict()
        pitchers["home_team"] = pitchers["team"].map(away_to_home)

    weather = weather.drop_duplicates(subset="home_team")
    merged = pd.merge(pitchers, weather, on="home_team", how="left")

    if "xwoba" not in merged.columns:
        merged["xwoba"] = 0.320

    merged["adj_xwoba_weather"] = merged["xwoba"] + ((merged["temperature"] - 70) * 0.001)
    return merged

def apply_park_adjustment(pitchers, games, label):
    if label == "home":
        pitchers["home_team"] = pitchers["team"]
    else:
        away_to_home = games.set_index("away_team")["home_team"].to_dict()
        pitchers["home_team"] = pitchers["team"].map(away_to_home)

    pitchers = pd.merge(pitchers, games[["home_team", "time_of_day"]], on="home_team", how="left")
    pitchers = pd.merge(pitchers, load_park_factors("day"), on="home_team", how="left")

    night_games = pitchers["time_of_day"] == "night"
    pitchers.loc[night_games, "Park Factor"] = pd.merge(
        pitchers[night_games],
        load_park_factors("night"),
        on="home_team",
        how="left"
    )["Park Factor_y"].values

    if "xwoba" not in pitchers.columns:
        pitchers["xwoba"] = 0.320

    pitchers["adj_xwoba_park"] = pitchers["xwoba"] * (pitchers["Park Factor"] / 100)
    pitchers["adj_xwoba_park"] = pitchers["adj_xwoba_park"].fillna(pitchers["xwoba"])
    return pitchers

def combine_adjustments(weather_df, park_df):
    return pd.merge(
        weather_df,
        park_df[["name", "team", "adj_xwoba_park"]],
        on=["name", "team"],
        how="inner"
    ).assign(adj_xwoba_combined=lambda df: (df["adj_xwoba_weather"] + df["adj_xwoba_park"]) / 2)

def save_outputs(df, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path / f"pitchers_{label}_adjusted.csv", index=False)

    top5 = df[["name", "team", "adj_xwoba_weather", "adj_xwoba_park", "adj_xwoba_combined"]] \
        .sort_values(by="adj_xwoba_combined").head(5)

    with open(out_path / f"log_combined_pitchers_{label}.txt", "w") as f:
        f.write(f"Top 5 pitchers ({label}) by lowest adjusted xwOBA:\n")
        f.write(top5.to_string(index=False))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/pitchers_*_adjusted.csv", "data/adjusted/log_combined_pitchers_*.txt"], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Auto-commit: Pitcher adjustments applied"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Adjusted pitcher files committed.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    weather, games = load_weather_and_games()

    for label in ["home", "away"]:
        df = pd.read_csv(f"data/adjusted/pitchers_{label}.csv")
        weather_adj = apply_weather_adjustment(df.copy(), weather, games, label)
        park_adj = apply_park_adjustment(df.copy(), games, label)
        combined = combine_adjustments(weather_adj, park_adj)
        print(f"✅ {label.title()} pitchers combined:", len(combined))
        save_outputs(combined, label)

    commit_outputs()

if __name__ == "__main__":
    main()
