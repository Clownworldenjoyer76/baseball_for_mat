# scripts/finalbatawp.py

import os
import subprocess
import pandas as pd

OUTPUT_DIR = "data/end_chain/final"
OUTPUT_FILE = "finalbatawp.csv"

GAMES_PATH = "data/end_chain/cleaned/games_today_cleaned.csv"
BATAWP_PATH = "data/end_chain/cleaned/bat_awp_cleaned.csv"


def _select_games_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select a safe subset of authoritative columns from games_today_cleaned.
    We merge on game_id and only bring what we need for the final file.
    """
    keep = [
        "game_id",               # merge key
        "home_team",
        "away_team",
        "game_time",
        "pitcher_home",
        "pitcher_away",
        "venue",
    ]
    existing = [c for c in keep if c in df.columns]
    return df[existing].copy()


def _tidy_suffixes(merged: pd.DataFrame, prefer_left: list) -> pd.DataFrame:
    """
    Resolve _x/_y columns cleanly:
      - For each base column in prefer_left, keep the left value if present,
        otherwise use the right value; then drop the suffixed columns.
      - For all other duplicated base columns, keep the left and drop the right.
    """
    df = merged.copy()

    # Identify base names that have _x/_y pairs
    bases = {}
    for col in df.columns:
        if col.endswith("_x") and f"{col[:-2]}_y" in df.columns:
            bases[col[:-2]] = True

    for base in bases:
        left = f"{base}_x"
        right = f"{base}_y"
        # Create base column deterministically
        if base in prefer_left:
            # Prefer left if not null; otherwise use right
            df[base] = df[left].combine_first(df[right])
        else:
            # Default: still prefer left (conservative)
            df[base] = df[left].combine_first(df[right])

        # Drop the suffixed columns
        df.drop(columns=[left, right], inplace=True, errors="ignore")

    return df


def final_bat_awp():
    """
    Build finalbatawp.csv by merging batting AWP with today's games on game_id.
    No weather merges. Output is de-suffixed and tidy.
    """
    # Load input files
    try:
        bat_awp_df = pd.read_csv(BATAWP_PATH)
    except FileNotFoundError:
        print(f"❌ Missing input: {BATAWP_PATH}")
        return
    except Exception as e:
        print(f"❌ Error reading {BATAWP_PATH}: {e}")
        return

    try:
        games_df = pd.read_csv(GAMES_PATH)
    except FileNotFoundError:
        print(f"❌ Missing input: {GAMES_PATH}")
        return
    except Exception as e:
        print(f"❌ Error reading {GAMES_PATH}: {e}")
        return

    if "game_id" not in bat_awp_df.columns:
        print("❌ 'game_id' not found in bat_awp_cleaned.csv. Cannot merge on game_id.")
        return
    if "game_id" not in games_df.columns:
        print("❌ 'game_id' not found in games_today_cleaned.csv. Cannot merge on game_id.")
        return

    games_keep = _select_games_cols(games_df)

    # Perform the merge on game_id
    merged = pd.merge(
        bat_awp_df,
        games_keep,
        on="game_id",
        how="left",
        suffixes=("_bat", "_games")  # temporary; we'll tidy next
    )

    # If any overlapping names existed (e.g., home_team/away_team/game_time),
    # pandas will NOT make _x/_y here because we used custom suffixes.
    # To still cover edge cases where the left frame already had those cols,
    # re-run with standard suffix logic only for the overlapping set.
    # Detect overlaps and, if any, redo a targeted merge just for them.
    overlap = set(bat_awp_df.columns).intersection(set(games_keep.columns)) - {"game_id"}
    if overlap:
        # Make a minimal merge to get standard _x/_y behavior for overlaps
        tmp = pd.merge(
            bat_awp_df[list({"game_id"} | overlap)],
            games_keep[list({"game_id"} | overlap)],
            on="game_id",
            how="left",
            suffixes=("_x", "_y"),
        )
        # Tidy _x/_y for those overlaps (prefer left/batting values by default)
        tmp = _tidy_suffixes(tmp, prefer_left=list(overlap))
        # Replace in 'merged' the overlapped base columns with the resolved ones
        for base in overlap:
            if base in tmp.columns:
                merged[base] = tmp[base]

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    # Write
    try:
        merged.to_csv(output_path, index=False)
        print(f"✅ Successfully created '{output_path}'")
    except Exception as e:
        print(f"❌ Error writing '{output_path}': {e}")
        return

    # Optional git commit/push of the single output file
    try:
        subprocess.run(["git", "add", output_path], check=True)
        subprocess.run(
            ["git", "commit", "-m", f"📊 Auto-update {OUTPUT_FILE} (join on game_id; no weather merges)"],
            check=True,
        )
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except FileNotFoundError:
        print("ℹ️ Git not found; skipping commit/push.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git step failed: {e}")


if __name__ == "__main__":
    final_bat_awp()
