def save_outputs(pitchers, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)

    # Normalize and clean pitcher/team columns for reliable deduplication
    pitchers["pitcher"] = pitchers["pitcher"].astype(str).str.strip().str.lower()
    pitchers["team"] = pitchers["team"].astype(str).str.strip().str.lower()

    # Sort to keep highest adj_woba_weather (handles NaNs safely)
    pitchers = pitchers.sort_values(by="adj_woba_weather", ascending=False, na_position='last')

    # Deduplicate
    before_dedup = len(pitchers)
    pitchers = pitchers.drop_duplicates(subset=["pitcher", "team"], keep="first")
    after_dedup = len(pitchers)

    print(f"🔁 Deduplication complete: {before_dedup} → {after_dedup} rows")

    if before_dedup > after_dedup:
        print(f"⚠️ {before_dedup - after_dedup} duplicates removed based on pitcher + team")

    # Save output CSV and log
    outfile = out_path / f"pitchers_{label}_weather.csv"
    logfile = out_path / f"log_pitchers_weather_{label}.txt"

    pitchers.to_csv(outfile, index=False)

    with open(logfile, 'w') as f:
        f.write("Top adjusted pitchers:\n")
        f.write(pitchers[['pitcher', 'team', 'adj_woba_weather']].head().to_string(index=False))

    print(f"✅ Saved: {outfile}")
    print(f"🧾 Log written: {logfile}")
