    # --- IN-PLACE UPDATE: only prop_correct in the per-day file ---
    out_path = Path(args.out)  # e.g., data/bets/bet_history/2025-08-08_player_props.csv
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Expected matching keys in your per‑day file
    KEYS = ["date", "team", "player_name", "prop_type"]

    # Aggregate actual outcomes we need to judge the prop; normalize columns
    actual = df.copy()
    # Normalize a few names to match your prop types
    actual["hits"] = actual.get("hits")
    actual["home_runs"] = actual.get("home_runs")
    actual["total_bases"] = actual.get("total_bases")
    actual["strikeouts"] = actual.get("strikeouts_pitcher").fillna(actual.get("strikeouts_batter"))
    actual["walks"] = actual.get("walks_pitcher").fillna(actual.get("walks_batter"))

    # Keep only fields we need to evaluate “Over line?”
    actual = actual.groupby(["team", "player_name"], as_index=False).agg({
        "hits":"max", "home_runs":"max", "total_bases":"max",
        "strikeouts":"max", "walks":"max"
    })

    # Load per‑day picks
    if not out_path.exists():
        raise SystemExit(f"Per-day player picks not found: {out_path}")
    picks = pd.read_csv(out_path)

    # Ensure keys exist
    for k in KEYS:
        if k not in picks.columns:
            picks[k] = ""

    # Join to get the actual metric that matches prop_type
    def value_for(row):
        ptype = str(row["prop_type"]).strip().lower()
        metric_map = {
            "hits": "hits",
            "home_runs": "home_runs",
            "total_bases": "total_bases",
            "strikeouts": "strikeouts",          # batter K props if any
            "pitcher_strikeouts": "strikeouts",  # pitcher K props
            "walks": "walks",                    # batter walks if any
            "walks_allowed": "walks",            # pitcher walks allowed
        }
        return metric_map.get(ptype, None)

    # Merge picks with actuals
    merged = picks.merge(actual, on=["team", "player_name"], how="left", suffixes=("", "_actual"))

    # Decide correctness: Over if actual >= line (line may be 'prop_line' or 'line')
    ln = merged["prop_line"] if "prop_line" in merged.columns else merged.get("line")
    # Build actual_value per row based on prop_type
    actual_vals = []
    for idx, row in merged.iterrows():
        metric = value_for(row)
        val = row.get(metric) if metric else None
        actual_vals.append(val)
    merged["__actual_value"] = actual_vals

    def decide(actual_value, line):
        try:
            if pd.isna(actual_value) or pd.isna(line):
                return ""
            return "Yes" if float(actual_value) >= float(line) else "No"
        except Exception:
            return ""

    merged["prop_correct"] = [decide(av, l) for av, l in zip(merged["__actual_value"], ln)]

    # Write back ONLY prop_correct
    picks["prop_correct"] = merged["prop_correct"]
    picks.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Updated {out_path} (wrote: prop_correct)")
