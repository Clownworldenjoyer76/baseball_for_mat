import argparse
import csv
import sys
from pathlib import Path
import pandas as pd

# Parse arguments
parser = argparse.ArgumentParser(description="Update daily game props file with actual_real_run_total, run_total_diff, favorite_correct.")
parser.add_argument(
    "--out",
    required=True,
    help="Path to the per-day game_props.csv to update (e.g., data/bets/bet_history/2025-08-08_game_props.csv)"
)
args = parser.parse_args()

out_path = Path(args.out)
if not out_path.exists():
    print(f"❌ ERROR: File not found: {out_path}", file=sys.stderr)
    sys.exit(1)

# Load the existing daily game props file
df = pd.read_csv(out_path)

# Ensure required columns exist
required_cols = ["date", "home_team", "away_team", "home_score", "away_score", "projected_real_run_total", "favorite"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    print(f"❌ ERROR: Missing required columns in {out_path}: {missing}", file=sys.stderr)
    sys.exit(1)

# Standardize date
df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

# Compute actual real run total
df["actual_real_run_total"] = (
    df["home_score"].astype(float) + df["away_score"].astype(float)
).where(df["home_score"].notna() & df["away_score"].notna(), None)

# Determine winner
df["winner"] = df.apply(
    lambda r: r["home_team"] if pd.notna(r["home_score"]) and pd.notna(r["away_score"]) and float(r["home_score"]) > float(r["away_score"])
    else (r["away_team"] if pd.notna(r["home_score"]) and pd.notna(r["away_score"]) else None),
    axis=1
)

# Compute run_total_diff
df["run_total_diff"] = (
    df["actual_real_run_total"].astype(float) - df["projected_real_run_total"].astype(float)
).where(
    df["actual_real_run_total"].notna() & df["projected_real_run_total"].notna(),
    None
)

# Compute favorite_correct
df["favorite_correct"] = df.apply(
    lambda r: ("Yes" if pd.notna(r.get("winner")) and pd.notna(r.get("favorite")) and r["winner"] == r["favorite"]
               else ("No" if pd.notna(r.get("winner")) and pd.notna(r.get("favorite")) else "")),
    axis=1
)

# Save back to same file
df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
print(f"✅ Updated {out_path} (wrote: actual_real_run_total, run_total_diff, favorite_correct)")
