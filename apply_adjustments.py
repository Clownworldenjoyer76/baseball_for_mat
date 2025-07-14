
import pandas as pd

def apply_adjustments(batters, weather, park_day, park_night, is_home=True):
    # Determine which column to use as the join key for park/weather data
    if is_home:
        join_key = 'home_team'
        batters[join_key] = batters['team']
    else:
        join_key = 'home_team'
        batters[join_key] = batters['opponent_team']

    # Deduplicate join sources
    weather = weather.drop_duplicates(subset='home_team')
    park_day = park_day.drop_duplicates(subset='home_team')
    park_night = park_night.drop_duplicates(subset='home_team')

    # Merge with weather data
    batters = pd.merge(batters, weather, on=join_key, how="left")

    # Merge with park factors
    batters = pd.merge(batters, park_day, on=join_key, how="left", suffixes=('', '_day'))
    batters = pd.merge(batters, park_night, on=join_key, how="left", suffixes=('', '_night'))

    # Adjustments — only if required columns exist
    if 'woba' in batters.columns and 'temperature' in batters.columns:
        batters['adj_woba'] = batters['woba'] + ((batters['temperature'] - 70) * 0.001)
    else:
        batters['adj_woba'] = batters.get('woba', 0)

    if 'home_run' in batters.columns and 'HR' in batters.columns:
        batters['adj_home_run'] = batters['home_run'] * batters['HR']
    else:
        batters['adj_home_run'] = batters.get('home_run', 0)

    if 'hard_hit_percent' in batters.columns and 'HardHit' in batters.columns:
        batters['adj_hard_hit_percent'] = (batters['hard_hit_percent'] / 100.0) * batters['HardHit']
    else:
        batters['adj_hard_hit_percent'] = batters.get('hard_hit_percent', 0)

    return batters
