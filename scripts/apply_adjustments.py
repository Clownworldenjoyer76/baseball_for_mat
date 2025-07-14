
import pandas as pd

def apply_adjustments(batters, weather, park_day, park_night):
    # Copy 'team' to 'home_team' so we can join on park and weather
    if 'team' in batters.columns:
        batters['home_team'] = batters['team']
    else:
        raise ValueError("Missing 'team' column in batters data. Tagging step may be incomplete.")

    # Deduplicate join sources by home_team
    weather = weather.drop_duplicates(subset='home_team')
    park_day = park_day.drop_duplicates(subset='home_team')
    park_night = park_night.drop_duplicates(subset='home_team')

    # Merge with weather data
    batters = pd.merge(batters, weather, on="home_team", how="left")

    # Merge with park factors
    batters = pd.merge(batters, park_day, on="home_team", how="left", suffixes=('', '_day'))
    batters = pd.merge(batters, park_night, on="home_team", how="left", suffixes=('', '_night'))

    # Fill missing woba with league average
    if 'woba' in batters.columns:
        missing_woba_count = batters['woba'].isna().sum()
        print(f"ℹ️ Filling {missing_woba_count} missing wOBA values with league average (0.320)")
        batters['woba'] = batters['woba'].fillna(0.320)
    else:
        print("⚠️ Column 'woba' missing entirely. Creating with default 0.320")
        batters['woba'] = 0.320

    if 'temperature' in batters.columns:
        batters['adj_woba'] = batters['woba'] + ((batters['temperature'] - 70) * 0.001)
    else:
        batters['adj_woba'] = batters['woba']

    if 'home_run' in batters.columns and 'HR' in batters.columns:
        batters['adj_home_run'] = batters['home_run'] * batters['HR']
    else:
        batters['adj_home_run'] = batters.get('home_run', 0)

    if 'hard_hit_percent' in batters.columns and 'HardHit' in batters.columns:
        batters['adj_hard_hit_percent'] = (batters['hard_hit_percent'] / 100.0) * batters['HardHit']
    else:
        batters['adj_hard_hit_percent'] = batters.get('hard_hit_percent', 0)

    return batters
