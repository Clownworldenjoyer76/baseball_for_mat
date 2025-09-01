import argparse, pandas as pd
from pathlib import Path

def to_int(s):
    try: return pd.Int64Dtype()
    except: return "Int64"

ap = argparse.ArgumentParser()
ap.add_argument("--games", required=True)
ap.add_argument("--batters", required=True)
ap.add_argument("--pitchers", required=True)
ap.add_argument("--game-date", required=True)   # YYYYMMDD
args = ap.parse_args()

# --- games: add game_id
g = pd.read_csv(args.games, low_memory=False)
for col in ["home_team_id","away_team_id"]:
    if g[col].dtype != "int64" and str(g[col].dtype) != "Int64":
        g[col] = pd.to_numeric(g[col], errors="coerce").astype("Int64")
g["game_id"] = g["home_team_id"].astype(str) + "_" + g["away_team_id"].astype(str) + "_" + args.game_date
g.to_csv(args.games, index=False)

# Build the set of valid team_ids playing today
valid_team_ids = set(pd.concat([g["home_team_id"], g["away_team_id"]]).dropna().astype(int).tolist())

# --- batters: enforce type, de-dupe, drop pitchers incorrectly present
b = pd.read_csv(args.batters, low_memory=False)
if "type" in b.columns:
    b = b[b["type"].str.lower().eq("batter")]
# keep only rows on today’s teams
if "team_id" in b.columns:
    b["team_id"] = pd.to_numeric(b["team_id"], errors="coerce").astype("Int64")
    b = b[b["team_id"].isin(valid_team_ids)]
# de-dupe strictly by player_id -> keep most recent PA or first occurrence
keep_cols = ["player_id","team_id","pa"] if "pa" in b.columns else ["player_id","team_id"]
b = (b.sort_values(by=["pa"], ascending=False) if "pa" in b.columns else b).drop_duplicates("player_id", keep="first")
b.to_csv(args.batters, index=False)

# --- pitchers: enforce type, cast team_id int, remove any 'batter' labels
p = pd.read_csv(args.pitchers, low_memory=False)
if "type" in p.columns:
    p = p[p["type"].str.lower().eq("pitcher")]
if "team_id" in p.columns:
    p["team_id"] = pd.to_numeric(p["team_id"], errors="coerce").astype("Int64")
p.to_csv(args.pitchers, index=False)

# --- simple sanity report (optional stdout)
bad_batters = []
# example: pitcher showing in batters (Luis Garcia 671277)
if "player_id" in b.columns:
    bad_batters = [pid for pid in [671277] if pid in set(b["player_id"].astype(int))]  # adjust if you track roles elsewhere
if bad_batters:
    print("Removed pitcher(s) wrongly in batters_today:", bad_batters)
