def apply_adjustments(pitchers_df, games_df, side):
    adjusted = []
    log_entries = []

    pitchers_df = normalize_columns(pitchers_df)
    games_df = normalize_columns(games_df)

    team_col = 'home_team' if side == 'home' else 'away_team'
    pitchers_df[team_col] = pitchers_df[team_col].astype(str).str.strip().str.lower()

    for _, row in games_df.iterrows():
        try:
            home_team = str(row['home_team']).strip()
            game_time = str(row['game_time']).strip()

            if home_team.lower() in ["", "undecided", "nan"] or game_time.lower() in ["", "nan"]:
                log_entries.append(
                    f"Skipping row due to invalid values — home_team: '{home_team}', game_time: '{game_time}'"
                )
                continue

            park_factors = load_park_factors(game_time)

            if 'home_team' not in park_factors.columns or 'Park Factor' not in park_factors.columns:
                log_entries.append("Park factors file is missing required columns.")
                continue

            park_factors['home_team'] = park_factors['home_team'].astype(str).str.strip().str.lower()
            park_row = park_factors[park_factors['home_team'] == home_team.lower()]

            if park_row.empty or pd.isna(park_row['Park Factor'].values[0]):
                log_entries.append(f"No park factor found for {home_team} at time {game_time}")
                continue

            park_factor = float(park_row['Park Factor'].values[0])
            team_pitchers = pitchers_df[pitchers_df[team_col] == home_team.lower()].copy()

            if team_pitchers.empty:
                log_entries.append(f"No pitcher data found for team: {home_team}")
                continue

            for stat in STATS_TO_ADJUST:
                if stat in team_pitchers.columns:
                    team_pitchers[stat] *= park_factor / 100
                else:
                    log_entries.append(f"Stat '{stat}' not found in pitcher data for {home_team}")

            adjusted.append(team_pitchers)
            log_entries.append(f"Adjusted {home_team} pitchers using park factor {park_factor}")
        except Exception as e:
            log_entries.append(f"Error processing row: {e}")
            continue

    if adjusted:
        result = pd.concat(adjusted)
        try:
            top5 = result[['name', team_col, STATS_TO_ADJUST[0]]].sort_values(by=STATS_TO_ADJUST[0], ascending=False).head(5)
            log_entries.append('\nTop 5 affected pitchers:')
            log_entries.append(top5.to_string(index=False))
        except Exception as e:
            log_entries.append(f"Failed to log top 5 pitchers: {e}")
    else:
        result = pd.DataFrame()
        log_entries.append("No teams matched. No adjustments applied.")

    return result, log_entries
