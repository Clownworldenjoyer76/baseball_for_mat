
# scripts/project_pitcher_props.py
# Keep as-is except ensure any columns needed for opponent context are preserved in the merged frame.

import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
import sys

# Inputs
BATTERS_IN = Path("data/end_chain/final/bat_today_final.csv")         # batter base (by matchup)
PITCHERS_XTRA = Path("data/end_chain/pitchers_xtra.csv")              # opponent pitcher rates (opp_K%, opp_BB% etc.)
PITCHERS_CLEAN = Path("data/cleaned/pitchers_normalized_cleaned.csv") # optional additional pitcher context
OUTPUT_FILE = Path("data/_projections/batter_props_projected_with_opp.csv")

# Columns we expect from pitcher context (to prefer)
EXPECT_OPP_COLS = {
    "opp_K%": ["opp_K%", "opp_k_percent", "opponent_k_percent"],
    "opp_BB%": ["opp_BB%", "opp_bb_percent", "opponent_bb_percent"],
}

def _resolve_any(df: pd.DataFrame, names):
    for n in names:
        if n in df.columns:
            return n
        for col in df.columns:
            if col.lower() == n.lower():
                return col
    return None

def main():
    try:
        bats = pd.read_csv(BATTERS_IN)
    except Exception as e:
        print(f"Failed to read {BATTERS_IN}: {e}")
        sys.exit(1)

    # Attempt to merge opponent context by a reasonable key (team or opponent pitcher id if available)
    # We try flexible keys to accommodate existing pipelines.
    # Priority: opponent pitcher id -> opponent team -> matchup join hints present in bats
    opp_frames = []
    for path in [PITCHERS_XTRA, PITCHERS_CLEAN]:
        if path.exists():
            try:
                opp_frames.append(pd.read_csv(path))
            except Exception as e:
                print(f"Warning: failed to read {path}: {e}")

    if opp_frames:
        opp = pd.concat(opp_frames, axis=0, ignore_index=True).drop_duplicates()
        # Try a set of joins; keep the first that works (non-empty)
        merged = None
        join_attempts = [
            # common keys from prior workflows (adjust if your schema differs)
            (["opp_pitcher_id"], ["player_id", "pitcher_id", "mlb_id"]),
            (["opp_team"], ["team", "opp_team", "opponent_team"]),
            (["game_id"], ["game_id"]),
        ]
        for left_keys, right_keys in join_attempts:
            left_keys = [k for k in left_keys if k in bats.columns]
            right_keys = [k for k in right_keys if k in opp.columns]
            if not left_keys or not right_keys:
                continue
            tmp = bats.merge(opp, left_on=left_keys[0], right_on=right_keys[0], how="left", suffixes=("", "_opp"))
            if len(tmp) > 0:
                merged = tmp
                break
        bats = merged if merged is not None else bats

        # Ensure the opponent columns are available with an accepted name
        for logical, aliases in EXPECT_OPP_COLS.items():
            present = _resolve_any(bats, aliases)
            if present is None:
                # Try to map from any similarly named column in the merged data
                src = _resolve_any(bats, [f"{logical}_opp", logical.replace('%','_percent')])
                if src is not None and src != logical:
                    bats[logical] = bats[src]

    # Now run projections (will fail fast if required batter columns are missing)
    df_proj = calculate_all_projections(bats)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_proj.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote: {OUTPUT_FILE} ({len(df_proj)} rows)")

if __name__ == "__main__":
    main()
