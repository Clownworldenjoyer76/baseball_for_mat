import pandas as pd

def apply_adjustments(batters, weather, park_day, park_night):
    # Merge weather (on 'venue')
    batters = pd.merge(batters, weather, on="venue", how="left")

    # Merge park factors (on 'stadium')
    batters = pd.merge(batters, park_day, on="stadium", how="left", suffixes=('', '_day'))
    batters = pd.merge(batters, park_night, on="stadium", how="left", suffixes=('', '_night'))

    # Placeholder adjustment logic
    if 'temp' in batters.columns:
        batters['temp_adjusted_wOBA'] = batters['wOBA'] + (batters['temp'] - 70) * 0.001
    else:
        batters['temp_adjusted_wOBA'] = batters['wOBA']

    return batters
