# scripts/bet_tracker.py
import os
import csv
import math
import tempfile
import shutil
from datetime import date
import re
import pandas as pd

# ---------- File paths ----------
BATTER_PROPS_FILE  = 'data/_projections/batter_props_z_expanded.csv'
PITCHER_PROPS_FILE = 'data/_projections/pitcher_mega_z.csv'   # player_id,name,team,prop_type,line,value,z_score,mega_z,over_probability
FINAL_SCORES_FILE  = 'data/_projections/final_scores_projected.csv'
BATTER_STATS_FILE  = 'data/cleaned/batters_today.csv'

PLAYER_PROPS_OUT = 'data/bets/player_props_history.csv'
GAME_PROPS_OUT   = 'data/bets/game_props_history.csv'

# ---------- MLB aliases (for matching only; output keeps original strings) ----------
MLB_CODES = {
    "ARI":"ARI","ARIZONA":"ARI","D-BACKS":"ARI","DBACKS":"ARI","DIAMONDBACKS":"ARI","AZ":"ARI",
    "ATL":"ATL","ATLANTA":"ATL","BRAVES":"ATL",
    "BAL":"BAL","BALTIMORE":"BAL","ORIOLES":"BAL","OS":"BAL",
    "BOS":"BOS","BOSTON":"BOS","RED SOX":"BOS","REDSOX":"BOS",
    "CHC":"CHC","CHICAGO CUBS":"CHC","CUBS":"CHC","CHN":"CHC",
    "CWS":"CWS","CHICAGO WHITE SOX":"CWS","WHITE SOX":"CWS","CHW":"CWS","WHITESOX":"CWS",
    "CIN":"CIN","CINCINNATI":"CIN","REDS":"CIN",
    "CLE":"CLE","CLEVELAND":"CLE","GUARDIANS":"CLE","INDIANS":"CLE",
    "COL":"COL","COLORADO":"COL","ROCKIES":"COL",
    "DET":"DET","DETROIT":"DET","TIGERS":"DET",
    "HOU":"HOU","HOUSTON":"HOU","ASTROS":"HOU",
    "KCR":"KCR","KC":"KCR","KANSAS CITY":"KCR","ROYALS":"KCR",
    "LAA":"LAA","LA ANGELS":"LAA","ANGELS":"LAA","ANAHEIM":"LAA",
    "LAD":"LAD","LA DODGERS":"LAD","DODGERS":"LAD",
    "MIA":"MIA","MIAMI":"MIA","MARLINS":"MIA","FLA":"MIA",
    "MIL":"MIL","MILWAUKEE":"MIL","BREWERS":"MIL",
    "MIN":"MIN","MINNESOTA":"MIN","TWINS":"MIN",
    "NYM":"NYM","NEW YORK METS":"NYM","METS":"NYM",
    "NYY":"NYY","NEW YORK YANKEES":"NYY","YANKEES":"NYY","YANKS":"NYY",
    "OAK":"OAK","OAKLAND":"OAK","ATHLETICS":"OAK","A'S":"OAK","AS":"OAK",
    "PHI":"PHI","PHILADELPHIA":"PHI","PHILLIES":"PHI","PHILS":"PHI",
    "PIT":"PIT","PITTSBURGH":"PIT","PIRATES":"PIT","BUCS":"PIT",
    "SD":"SDP","SDP":"SDP","SAN DIEGO":"SDP","PADRES":"SDP",
    "SEA":"SEA","SEATTLE":"SEA","MARINERS":"SEA","MS":"SEA",
    "SF":"SFG","SFG":"SFG","SAN FRANCISCO":"SFG","GIANTS":"SFG",
    "STL":"STL","ST. LOUIS":"STL","SAINT LOUIS":"STL","CARDINALS":"STL","CARDS":"STL",
    "TB":"TBR","TBR":"TBR","TAMPA BAY":"TBR","RAYS":"TBR",
    "TEX":"TEX","TEXAS":"TEX","RANGERS":"TEX",
    "TOR":"TOR","TORONTO":"TOR","BLUE JAYS":"TOR","JAYS":"TOR",
    "WSH":"WSH","WSN":"WSH","WASHINGTON":"WSH","NATIONALS":"WSH","NATS":"WSH"
}
MLB_VALID = set(MLB_CODES.values())

def _team_key(val: str) -> str:
    """Canonical key for matching (not for output)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip().upper()
    s = re.sub(r"\s+", " ", s)
    # Try exact 3-letter code
    if s in MLB_VALID:
        return s
    # Direct alias match
    if s in MLB_CODES:
        return MLB_CODES[s]
    # Token containment (e.g., 'Arizona Diamondbacks')
    for k, v in MLB_CODES.items():
        if k in s:
            return v
    # Compact (letters only) fallback
    s2 = re.sub(r"[^A-Z]", "", s)
    if s2 in MLB_CODES:
        return MLB_CODES[s2]
    if s2 in MLB_VALID:
        return s2
    # final: short tokens like 'AZ'
    if len(s2) <= 3 and len(s2) > 0:
        return MLB_CODES.get(s2, s2)
    return s2

# ---------- Utils ----------
def ensure_directory_exists(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def _as_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _prob_from_z(z):
    """Soft logistic to convert z-ish scores to a probability (fallback only)."""
    try:
        p = 1 / (1 + math.exp(-(float(z) or 0) * 0.9))
    except Exception:
        p = 0.65
    return max(0.50, min(0.98, p))

def _prob_from_proj_line(prop_type: str, projection, line):
    """Heuristic: derive probability from projection vs line when explicit prob is missing/weak."""
    proj = _as_float(projection); ln = _as_float(line)
    if proj is None or ln is None:
        return None
    slopes = {"home_runs":2.2,"hits":1.4,"total_bases":1.2,"pitcher_strikeouts":0.9,"walks_allowed":1.0}
    k = slopes.get((prop_type or "").strip().lower(), 1.0)
    delta = proj - ln
    try:
        prob = 1 / (1 + math.exp(-k * delta))
    except Exception:
        prob = 0.5
    return max(0.50, min(0.98, prob))

def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def _pick_col(df: pd.DataFrame, options: list[str]) -> str | None:
    cols = set(df.columns)
    for c in options:
        if c in cols:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in options:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def _schema_snapshot(label: str, df: pd.DataFrame, required: list[str], preferred: list[str]=None):
    preferred = preferred or []
    have = set(df.columns)
    found_req = [c for c in required if c in have]
    missing_req = [c for c in required if c not in have]
    found_pref = [c for c in preferred if c in have]
    extras = sorted(list(have - set(required) - set(preferred)))
    print(f"🔎 {label} schema → required found={found_req} missing={missing_req} preferred found={found_pref} extras(sample)={extras[:8]}")

def _write_csv_atomic(path: str, df: pd.DataFrame, header: bool):
    ensure_directory_exists(path)
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="bettracker_", suffix=".csv")
    os.close(tmp_fd)
    try:
        df.to_csv(tmp_path, index=False, header=header, quoting=csv.QUOTE_ALL)
        shutil.move(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

def _round_cols(df: pd.DataFrame, cols_prec: dict):
    df = df.copy()
    for c, prec in cols_prec.items():
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').round(prec)
    return df

# ---------- Pitcher props loader ----------
def load_pitcher_props_simple(pitcher_df_raw: pd.DataFrame) -> pd.DataFrame:
    if pitcher_df_raw is None or pitcher_df_raw.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])
    df = _std_cols(pitcher_df_raw)
    if 'prop_type' not in df.columns:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    mask = df['prop_type'].astype(str).str.strip().str.lower().isin(['strikeouts', 'walks'])
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame(columns=['name','team','prop_type','line','over_probability','projection','player_id'])

    df['prop_type'] = df['prop_type'].astype(str).str.strip().str.lower().map({
        'strikeouts':'pitcher_strikeouts','walks':'walks_allowed'
    })
    df['line'] = pd.to_numeric(df.get('line', pd.NA), errors='coerce')

    provided_prob = pd.to_numeric(df.get('over_probability', pd.NA), errors='coerce')
    needs_fill = provided_prob.isna() | (provided_prob <= 0.5)

    z_src = None
    if 'mega_z' in df.columns:
        z_src = pd.to_numeric(df['mega_z'], errors='coerce')
    elif 'z_score' in df.columns:
        z_src = pd.to_numeric(df['z_score'], errors='coerce')

    df['over_probability'] = provided_prob
    if z_src is not None:
        df.loc[needs_fill, 'over_probability'] = z_src.apply(_prob_from_z)[needs_fill]

    df['projection'] = pd.to_numeric(df.get('value', pd.Series([None]*len(df))), errors='coerce')
    for c in ['name','team','player_id']:
        if c not in df.columns: df[c] = ''

    out = df[['name','team','prop_type','line','over_probability','projection','player_id']].copy()
    need_infer = out['over_probability'].isna() | (out['over_probability'] <= 0.5)
    if need_infer.any():
        out.loc[need_infer, 'over_probability'] = out.loc[need_infer].apply(
            lambda r: _prob_from_proj_line(r['prop_type'], r['projection'], r['line']) or 0.5, axis=1
        )
    out['over_probability'] = pd.to_numeric(out['over_probability'], errors='coerce').clip(0.50, 0.98)
    out = out.dropna(subset=['name','team','prop_type','line','over_probability','projection'])
    out = out.drop_duplicates(subset=['name','prop_type','line'], keep='first')
    return out

# ---------- Thresholds ----------
def _market_threshold(prop_type: str) -> float:
    p = (prop_type or "").lower().strip()
    if p == "home_runs": return 0.15
    if p == "hits": return 0.60
    if p == "total_bases": return 1.20
    if p == "pitcher_strikeouts": return 3.0
    if p == "walks_allowed": return 0.8
    return 0.2

def run_bet_tracker():
    # Load
    try:
        batter_df     = _std_cols(pd.read_csv(BATTER_PROPS_FILE))
        pitcher_df    = _std_cols(pd.read_csv(PITCHER_PROPS_FILE))
        games_df      = _std_cols(pd.read_csv(FINAL_SCORES_FILE))
        batter_stats  = _std_cols(pd.read_csv(BATTER_STATS_FILE))
    except FileNotFoundError as e:
        print(f"❌ Required input file not found - {e}")
        return
    except Exception as e:
        print(f"❌ Failed reading inputs - {e}")
        return

    # Schema logs
    _schema_snapshot("batter_props", batter_df, ['name','team','prop_type','line','over_probability','projection'], ['player_id'])
    _schema_snapshot("pitcher_props", pitcher_df, ['name','team','prop_type','line'], ['over_probability','value','player_id'])
    _schema_snapshot("final_scores", games_df, ['home_team','away_team'], ['home_score','away_score','date','game_date'])
    _schema_snapshot("batter_stats", batter_stats, ['player_id'], ['ab','AB','hit','home_run'])

    # Date
    date_col = _pick_col(games_df, ['date','Date','game_date'])
    if not date_col:
        current_date = str(date.today())
        print(f"⚠️ No date column; using today: {current_date}")
    else:
        vc = games_df[date_col].dropna().value_counts()
        current_date = vc.index[0] if not vc.empty else str(date.today())
        if len(vc) > 1:
            print(f"⚠️ Multiple dates; using most frequent: {current_date} (counts={vc.to_dict()})")
    print(f"📅 Using date: {current_date}")

    # Build matching keys (keep original team strings for output)
    h_col = _pick_col(games_df, ['home_team'])
    a_col = _pick_col(games_df, ['away_team'])
    games_df = games_df.copy()
    if h_col: games_df["__home_key__"] = games_df[h_col].apply(_team_key)
    if a_col: games_df["__away_key__"] = games_df[a_col].apply(_team_key)

    # Batters: AB/rate filtering
    if 'player_id' in batter_stats.columns:
        batter_stats["player_id"] = batter_stats["player_id"].astype(str).str.strip()
    if 'player_id' in batter_df.columns:
        batter_df["player_id"] = batter_df["player_id"].astype(str).str.strip()

    ab_col   = _pick_col(batter_stats, ['ab','AB','at_bats'])
    hit_col  = _pick_col(batter_stats, ['hit','hits'])
    hr_col   = _pick_col(batter_stats, ['home_run','home_runs','HR'])
    if ab_col and hit_col and hr_col and 'player_id' in batter_stats.columns and 'player_id' in batter_df.columns:
        tmp = batter_df.merge(batter_stats[["player_id", ab_col, hit_col, hr_col]], on="player_id", how="left")
        for c in (ab_col,hit_col,hr_col): tmp[c] = pd.to_numeric(tmp[c], errors='coerce')
        tmp["hr_rate"]  = (tmp[hr_col] / tmp[ab_col]).replace([float('inf')], 0).fillna(0)
        tmp["hit_rate"] = (tmp[hit_col] / tmp[ab_col]).replace([float('inf')], 0).fillna(0)
        tmp = tmp[(tmp[ab_col].isna()) | (tmp[ab_col] >= 20)].copy()
        def _b_ok(r):
            p = str(r.get("prop_type") or "").strip().lower()
            if p == "home_runs": return (r.get("hr_rate") or 0) >= 0.02
            if p in ("hits","total_bases"): return (r.get("hit_rate") or 0) >= 0.20
            return True
        batter_df = tmp[tmp.apply(_b_ok, axis=1)].copy()

    # Ensure core columns exist
    for col in ["name","team","prop_type","line","over_probability","projection"]:
        if col not in batter_df.columns: batter_df[col] = pd.NA

    # Infer batter probabilities if weak/missing
    if {"projection","line","prop_type"}.issubset(batter_df.columns):
        need_bp = batter_df["over_probability"].isna() | (pd.to_numeric(batter_df["over_probability"], errors='coerce') <= 0.5)
        if need_bp.any():
            batter_df.loc[need_bp, "over_probability"] = batter_df.loc[need_bp].apply(
                lambda r: _prob_from_proj_line(str(r["prop_type"]).lower(), r["projection"], r["line"]) or 0.5, axis=1
            )
    batter_df["over_probability"] = pd.to_numeric(batter_df["over_probability"], errors='coerce').clip(0.50, 0.98)
    batter_df["source"] = "batter"

    # Pitchers
    pitcher_std = load_pitcher_props_simple(pitcher_df)
    pitcher_std["source"] = "pitcher"

    # Combine
    keep_cols = ['name','team','prop_type','line','over_probability','projection','source']
    combined = pd.concat([batter_df[keep_cols], pitcher_std[keep_cols]], ignore_index=True)

    # Thresholds
    combined = combined[
        combined.apply(lambda r: _as_float(r["projection"]) is not None and _as_float(r["projection"]) >= _market_threshold(r["prop_type"]), axis=1)
    ]
    combined['over_probability'] = pd.to_numeric(combined['over_probability'], errors='coerce').clip(0.50, 0.98)
    combined = combined.dropna(subset=["over_probability"])
    combined = combined.sort_values("over_probability", ascending=False).drop_duplicates(subset=["name","prop_type","line"])

    # Drop rows missing numeric line (prevents empty prop_line)
    combined['line'] = pd.to_numeric(combined['line'], errors='coerce')
    combined = combined.dropna(subset=['line']).copy()

    # Best Props (diversity: max 1 per player)
    best_props_df, used = [], set()
    for _, row in combined.iterrows():
        nm = str(row["name"]).strip()
        if nm in used: continue
        best_props_df.append(row)
        used.add(nm)
        if len(best_props_df) >= 3: break
    best_props_df = pd.DataFrame(best_props_df) if best_props_df else pd.DataFrame(columns=combined.columns)
    best_props_df["bet_type"] = "Best Prop"
    best_keys = set(zip(best_props_df["name"], best_props_df["prop_type"], best_props_df["line"]))

    # Per-game: up to 3 per team per matchup (match on team keys, keep original strings in output)
    individual_props_df = pd.DataFrame()
    if h_col and a_col:
        games_unique = games_df.drop_duplicates(subset=[h_col, a_col]).copy()
        remaining = combined[~combined.apply(lambda r: (r["name"], r["prop_type"], r["line"]) in best_keys, axis=1)]
        # Build keys for props
        remaining = remaining.copy()
        remaining["__team_key__"] = remaining["team"].apply(_team_key)

        per_game = []
        total_added = 0
        for _, g in games_unique.iterrows():
            hk, ak = g.get("__home_key__",""), g.get("__away_key__","")
            gp = remaining[(remaining["__team_key__"] == hk) | (remaining["__team_key__"] == ak)].copy()
            if gp.empty: continue
            gp = gp.sort_values("over_probability", ascending=False).head(20)
            gp_home = gp[gp["__team_key__"] == hk].head(3)
            gp_away = gp[gp["__team_key__"] == ak].head(3)
            gp_bal = pd.concat([gp_home, gp_away]).sort_values("over_probability", ascending=False).head(5)
            if not gp_bal.empty:
                gp_bal["bet_type"] = "Individual Game"
                per_game.append(gp_bal.drop(columns=["__team_key__"], errors="ignore"))
                total_added += len(gp_bal)
        if per_game:
            individual_props_df = pd.concat(per_game, ignore_index=True)
        print(f"🎯 per-game selections created for {len(per_game)} matchups; rows added: {total_added}")
    else:
        print("ℹ️ Skipping per-game selections (missing team columns).")

    # Build player props output (rename to requested headers; keep original team strings)
    all_props = pd.concat([best_props_df, individual_props_df], ignore_index=True)

    # Strip whitespace in key text fields
    for col in ["name","team","prop_type"]:
        if col in all_props.columns:
            all_props[col] = all_props[col].astype(str).str.strip()

    all_props = _round_cols(all_props, {"over_probability": 4, "projection": 2, "line": 2})
    all_props = all_props.sort_values(["bet_type","over_probability","name"], ascending=[False, False, True])
    all_props["date"] = current_date

    # Ensure player_name/prop_line populated
    player_props_to_save = all_props[["date","name","team","line","prop_type","bet_type"]].copy()
    player_props_to_save.rename(columns={"name":"player_name","line":"prop_line"}, inplace=True)
    player_props_to_save["player_name"] = player_props_to_save["player_name"].fillna("").astype(str).str.strip()
    player_props_to_save["prop_line"] = pd.to_numeric(player_props_to_save["prop_line"], errors='coerce')
    player_props_to_save = player_props_to_save.dropna(subset=["player_name","prop_line"])
    player_props_to_save["prop_correct"] = ""

    # Write (fresh files after your deletion)
    ensure_directory_exists(PLAYER_PROPS_OUT)
    _write_csv_atomic(
        PLAYER_PROPS_OUT,
        player_props_to_save[["date","player_name","team","prop_line","prop_type","bet_type","prop_correct"]],
        header=True
    )

    # Game props (keep original strings for team names)
    hs_col = _pick_col(games_df, ['home_score','home_projection','home_proj','proj_home','home'])
    as_col = _pick_col(games_df, ['away_score','away_projection','away_proj','proj_away','away'])
    if h_col and a_col and hs_col and as_col:
        games_unique = games_df.drop_duplicates(subset=[h_col, a_col]).copy()
        # Always fill date
        games_unique["date"] = current_date

        hs = pd.to_numeric(games_unique.get(hs_col, pd.NA), errors='coerce')
        aw = pd.to_numeric(games_unique.get(as_col, pd.NA), errors='coerce')

        games_unique['favorite'] = games_unique.apply(
            lambda row: row[h_col] if _as_float(row.get(hs_col)) is not None and _as_float(row.get(as_col)) is not None
            and float(row[hs_col]) > float(row[as_col]) else row[a_col],
            axis=1
        )

        game_props_to_save = games_unique[['date', h_col, a_col]].copy()
        game_props_to_save.columns = ['date','home_team','away_team']
        game_props_to_save['favorite'] = games_unique['favorite'].values
        game_props_to_save['favorite_correct'] = ''
        game_props_to_save['projected_real_run_total'] = (hs + aw).round(2)
        game_props_to_save['actual_real_run_total'] = ''
        game_props_to_save['run_total_diff'] = ''
        game_props_to_save = game_props_to_save.sort_values(["date","home_team","away_team"]).copy()

        ensure_directory_exists(GAME_PROPS_OUT)
        _write_csv_atomic(
            GAME_PROPS_OUT,
            game_props_to_save[[
                "date","home_team","away_team","favorite","favorite_correct",
                "projected_real_run_total","actual_real_run_total","run_total_diff"
            ]],
            header=True
        )

    print(f"✅ Finished bet tracker for date: {current_date}")

if __name__ == '__main__':
    run_bet_tracker()
