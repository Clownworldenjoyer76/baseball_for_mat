    # --- IN-PLACE UPDATE: only 3 columns in the per-day file ---
    out_path = Path(args.out)  # e.g., data/bets/bet_history/2025-08-08_game_props.csv
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Keys to match on
    KEYS = ["date", "home_team", "away_team"]

    # Compute just the 3 fields we’re allowed to write
    minimal = scored.copy()
    minimal["date"] = pd.to_datetime(minimal["date"]).dt.strftime("%Y-%m-%d")
    minimal["actual_real_run_total"] = (
        minimal["home_score"].astype(float) + minimal["away_score"].astype(float)
    ).where(minimal["home_score"].notna() & minimal["away_score"].notna(), None)

    # favorite_correct depends on who actually won vs ‘favorite’ already in file
    minimal["winner"] = minimal.apply(
        lambda r: r["home_team"] if pd.notna(r["home_score"]) and pd.notna(r["away_score"]) and float(r["home_score"]) > float(r["away_score"])
        else (r["away_team"] if pd.notna(r["home_score"]) and pd.notna(r["away_score"]) else None),
        axis=1
    )

    # Load existing per‑day file; if not present, we still create it but only with allowed columns
    if out_path.exists():
        base = pd.read_csv(out_path)
    else:
        base = minimal[KEYS].drop_duplicates().copy()
        base["projected_real_run_total"] = None
        base["favorite"] = None

    # Merge to compute run_total_diff and favorite_correct using base's favorite & projected
    merged = base.merge(
        minimal[KEYS + ["actual_real_run_total", "winner"]],
        on=KEYS, how="left"
    )

    merged["run_total_diff"] = (
        merged["actual_real_run_total"].astype(float) - merged["projected_real_run_total"].astype(float)
    ).where(
        merged["actual_real_run_total"].notna() & merged["projected_real_run_total"].notna(),
        None
    )

    merged["favorite_correct"] = merged.apply(
        lambda r: ("Yes" if pd.notna(r.get("winner")) and pd.notna(r.get("favorite")) and r["winner"] == r["favorite"]
                   else ("No" if pd.notna(r.get("winner")) and pd.notna(r.get("favorite")) else "")),
        axis=1
    )

    # Write back ONLY the 3 columns (plus existing untouched columns)
    base.loc[:, "actual_real_run_total"] = merged["actual_real_run_total"]
    base.loc[:, "run_total_diff"] = merged["run_total_diff"]
    base.loc[:, "favorite_correct"] = merged["favorite_correct"]

    base.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Updated {out_path} (wrote: actual_real_run_total, run_total_diff, favorite_correct)")
