import pandas as pd
import numpy as np

def project_prep():
    PATHS = {
        "bat_today_final": "data/end_chain/final/bat_today_final.csv",
        "batter_away_final": "data/end_chain/final/batter_away_final.csv",
        "batter_home_final": "data/end_chain/final/batter_home_final.csv",
        "startingpitchers_final": "data/end_chain/final/startingpitchers_final.csv",
        "pitchers_normalized_cleaned": "data/cleaned/pitchers_normalized_cleaned.csv",
        "weather_input": "data/weather_input.csv",
        "weather_adjustments": "data/weather_adjustments.csv",
        "stadium_metadata": "data/Data/stadium_metadata.csv",
        "final_scores_projected": "data/_projections/final_scores_projected.csv"
    }

    try:
        df_bat_today = pd.read_csv(PATHS["bat_today_final"])
        df_batter_away = pd.read_csv(PATHS["batter_away_final"])
        df_batter_home = pd.read_csv(PATHS["batter_home_final"])
        df_startingpitchers = pd.read_csv(PATHS["startingpitchers_final"])
        df_pitchers_normalized = pd.read_csv(PATHS["pitchers_normalized_cleaned"])
        df_weather_input = pd.read_csv(PATHS["weather_input"])
        df_weather_adjustments = pd.read_csv(PATHS["weather_adjustments"])
        df_stadium_metadata = pd.read_csv(PATHS["stadium_metadata"])
        try:
            df_final_scores_projected = pd.read_csv(PATHS["final_scores_projected"])
        except FileNotFoundError:
            df_final_scores_projected = pd.DataFrame()
    except Exception as e:
        print(f"Load error: {e}")
        return

    if 'player_id' in df_bat_today.columns:
        for target_df in [df_batter_away, df_batter_home]:
            for col in ['b_total_bases', 'b_rbi']:
                if col not in target_df.columns:
                    target_df[col] = np.nan
                if col in df_bat_today.columns:
                    target_df = target_df.merge(
                        df_bat_today[['player_id', col]],
                        on='player_id',
                        how='left',
                        suffixes=('', '_from_bat_today')
                    )
                    target_df[col] = target_df[col].fillna(target_df[f'{col}_from_bat_today'])
                    target_df.drop(columns=[f'{col}_from_bat_today'], inplace=True)
            if target_df is df_batter_away:
                df_batter_away = target_df
            else:
                df_batter_home = target_df

    for col in [
        'Park Factor_input', 'city_input', 'is_dome_input', 'state_input',
        'time_of_day_input', 'timezone_input', 'team', 'pitcher_home', 'pitcher_away'
    ]:
        if col in df_batter_home.columns:
            df_batter_home.drop(columns=[col], inplace=True)

    for col in ['pitcher_away', 'pitcher_home']:
        if col in df_batter_away.columns:
            df_batter_away.drop(columns=[col], inplace=True)

    for col in ['last_name_first_name', 'year', 'team_xtra']:
        if col in df_startingpitchers.columns:
            df_startingpitchers.drop(columns=[col], inplace=True)

    for col in ['home_run', 'park_factor', 'weather_factor', 'player_id']:
        if col not in df_startingpitchers.columns:
            df_startingpitchers[col] = np.nan

    df_weather_input_melted = pd.melt(
        df_weather_input,
        id_vars=['Park Factor'],
        value_vars=['home_team', 'away_team'],
        var_name='team_type',
        value_name='team_match'
    )[['Park Factor', 'team_match']].drop_duplicates()

    df_startingpitchers = df_startingpitchers.merge(
        df_weather_input_melted,
        left_on='team',
        right_on='team_match',
        how='left'
    )
    df_startingpitchers['park_factor'] = df_startingpitchers['park_factor'].fillna(df_startingpitchers['Park Factor'])
    df_startingpitchers.drop(columns=['Park Factor', 'team_match'], inplace=True)

    df_weather_adjustments_melted = pd.melt(
        df_weather_adjustments,
        id_vars=['weather_factor'],
        value_vars=['home_team', 'away_team'],
        var_name='team_type',
        value_name='team_match'
    )[['weather_factor', 'team_match']].drop_duplicates()

    df_startingpitchers = df_startingpitchers.merge(
        df_weather_adjustments_melted,
        left_on='team',
        right_on='team_match',
        how='left'
    )
    if 'weather_factor' not in df_startingpitchers.columns:
        df_startingpitchers['weather_factor'] = np.nan
    df_startingpitchers['weather_factor'] = df_startingpitchers['weather_factor'].fillna(df_startingpitchers['weather_factor_y'])
    df_startingpitchers.drop(columns=['weather_factor_y', 'team_match'], inplace=True)

    if 'last_name, first_name' in df_startingpitchers.columns and 'name' in df_pitchers_normalized.columns:
        df_startingpitchers = df_startingpitchers.merge(
            df_pitchers_normalized[['name', 'player_id']],
            left_on='last_name, first_name',
            right_on='name',
            how='left',
            suffixes=('', '_from_norm')
        )
        df_startingpitchers['player_id'] = df_startingpitchers['player_id'].fillna(df_startingpitchers['player_id_from_norm'])
        df_startingpitchers.drop(columns=['name', 'player_id_from_norm'], inplace=True)

    if 'last_name, first_name' in df_startingpitchers.columns and 'name' in df_pitchers_normalized.columns:
        df_startingpitchers = df_startingpitchers.merge(
            df_pitchers_normalized[['name', 'home_run']],
            left_on='last_name, first_name',
            right_on='name',
            how='left',
            suffixes=('', '_from_norm')
        )
        df_startingpitchers['home_run'] = df_startingpitchers['home_run'].fillna(df_startingpitchers['home_run_from_norm'])
        df_startingpitchers.drop(columns=['home_run_from_norm', 'name'], errors='ignore', inplace=True)

    if {'home_team', 'away_team'}.issubset(df_stadium_metadata.columns):
        if df_final_scores_projected.empty:
            df_final_scores_projected = df_stadium_metadata[['home_team', 'away_team']].copy()
        else:
            for col in ['home_team', 'away_team']:
                df_final_scores_projected[col] = df_stadium_metadata[col].copy()

    df_batter_away.to_csv(PATHS["batter_away_final"], index=False)
    df_batter_home.to_csv(PATHS["batter_home_final"], index=False)
    df_startingpitchers.to_csv(PATHS["startingpitchers_final"], index=False)
    df_final_scores_projected.to_csv(PATHS["final_scores_projected"], index=False)

if __name__ == "__main__":
    project_prep()
