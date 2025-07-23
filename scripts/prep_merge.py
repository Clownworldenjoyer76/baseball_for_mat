def fix_pitcher_names_strict():
    # Load valid names
    normalized = pd.read_csv(normalized_pitchers_path)
    valid_names = normalized["last_name, first_name"].dropna().unique()

    # Create a raw-to-clean map by stripping and upper-casing for match
    name_map = {
        raw.strip().replace(",", "").upper(): valid
        for valid in valid_names
        for raw in [valid]
    }

    def map_name(name):
        if pd.isna(name): return name
        key = name.strip().replace(",", "").upper()
        return name_map.get(key, name)

    # Update home pitcher file
    ph = pd.read_csv(pitchers_home_path)
    if "last_name, first_name" in ph.columns:
        ph["last_name, first_name"] = ph["last_name, first_name"].apply(map_name)
    ph.to_csv(pitchers_home_path, index=False)

    # Update away pitcher file
    pa = pd.read_csv(pitchers_away_path)
    if "last_name, first_name" in pa.columns:
        pa["last_name, first_name"] = pa["last_name, first_name"].apply(map_name)
    pa.to_csv(pitchers_away_path, index=False)

    # Update todaysgames file
    games = pd.read_csv(games_path)
    if "pitcher_home" in games.columns:
        games["pitcher_home"] = games["pitcher_home"].apply(map_name)
    if "pitcher_away" in games.columns:
        games["pitcher_away"] = games["pitcher_away"].apply(map_name)
    games.to_csv(games_path, index=False)
