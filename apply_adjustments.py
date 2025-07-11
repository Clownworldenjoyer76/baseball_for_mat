
import pandas as pd

def apply_adjustments(batters, weather, park_day, park_night):
    # Merge weather (on home_team)
    if 'home_team' in batters.columns and 'home_team' in weather.columns:
        batters = pd.merge(batters, weather, on="home_team", how="left")

    # Merge park factors (on home_team)
    if 'home_team' in batters.columns and 'home_team' in park_day.columns:
        batters = pd.merge(batters, park_day, on="home_team", how="left", suffixes=('', '_day'))
    if 'home_team' in batters.columns and 'home_team' in park_night.columns:
        batters = pd.merge(batters, park_night, on="home_team", how="left", suffixes=('', '_night'))

    # Apply temperature-based adjustment to wOBA if both columns exist
    if 'temperature' in batters.columns and 'wOBA' in batters.columns:
        batters['temp_adjusted_wOBA'] = batters['wOBA'] + (batters['temperature'] - 70) * 0.001
    else:
        batters['temp_adjusted_wOBA'] = batters.get('wOBA', 0)

    return batters
