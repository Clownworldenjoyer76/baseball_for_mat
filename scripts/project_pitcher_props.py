#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from projection_formulas import calculate_all_projections

ENRICHED_FILE   = Path("data/raw/startingpitchers_with_opp_context.csv")
PITCHERS_BASE   = Path("data/end_chain/final/startingpitchers_final.csv")
PITCHERS_XTRA   = Path("data/end_chain/pitchers_xtra.csv")
PITCHERS_CLEAN  = Path("data/cleaned/pitchers_normalized_cleaned.csv")
OUTPUT_FILE     = Path("data/_projections/pitcher_props_projected.csv")

EXPECT_OPP_COLS = {
    "opp_K%":  ["opp_K%", "opp_k_percent", "opponent_k_percent", "k_percent_opp", "k%_opp"],
    "opp_BB%": ["opp_BB%", "opp_bb_percent", "opponent_bb_percent", "bb_percent_opp", "bb%_opp"],
}

JOIN_ATTEMPTS = [
    (["opponent_pitcher_id", "opp_pitcher_id"], ["player_id", "pitcher_id", "mlb_id"]),
    (["opponent_team", "opp_team"], ["team", "opp_team", "opponent_team"]),
    (["game_id"], ["game_id"]),
    (["player_id", "pitcher_id", "mlb_id"], ["player_id", "pitcher_id", "mlb_id"]),
]

def _resolve_any(df: pd.DataFrame, names):
    for n in names:
        if n in df.columns: return n
        n_low = str(n).lower()
        for c in df.columns:
            if str(c).lower() == n_low:
                return c
    return None

def _as_str(s: pd.Series):
    return s.astype(str).str.strip()

def _best_merge(base: pd.DataFrame, aux: pd.DataFrame) -> pd.DataFrame:
    best = {"score": -1, "merged": None}
    for left_candidates, right_candidates in JOIN_ATTEMPTS:
        left_key = next((k for k in left_candidates if k in base.columns), None)
        right_key = next((k for k in right_candidates if k in aux.columns), None)
        if not left_key or not right_key: continue
        b = base.copy(); a = aux.copy()
        b[left_key] = _as_str(b[left_key]); a[right_key] = _as_str(a[right_key])
        merged = b.merge(a, left_on=left_key, right_on=right_key, how="left", suffixes=("", "_aux"))
        cand = []
        for logical, aliases in EXPECT_OPP_COLS.items():
            for col in [logical] + aliases + [f"{logical}_aux"] + [f"{al}_aux" for al in aliases]:
                if col in merged.columns: cand.append(col)
        score = int((merged[cand].notna().sum(axis=1) > 0).sum()) if cand else 0
        if score > best["score"]:
            best = {"score": score, "merged": merged}
    return best["merged"] if best["merged"] is not None else base

def _standardize_opponent_cols(df: pd.DataFrame) -> pd.DataFrame:
    for logical, aliases in EXPECT_OPP_COLS.items():
        src = _resolve_any(df, [logical] + aliases + [f"{logical}_aux"] + [f"{a}_aux" for a in aliases])
        if src and src != logical:
            df[logical] = df[src]
    return df

def _load_base_with_fallbacks() -> pd.DataFrame:
    if not PITCHERS_BASE.exists():
        print(f"ERROR: Missing base: {PITCHERS_BASE}", file=sys.stderr)
        sys.exit(1)
    base = pd.read_csv(PITCHERS_BASE)
    aux_frames = []
    for p in (PITCHERS_XTRA, PITCHERS_CLEAN):
        if p.exists():
            try: aux_frames.append(pd.read_csv(p))
            except Exception as e: print(f"WARN: Failed to read {p}: {e}")
    if aux_frames:
        aux = pd.concat(aux_frames, ignore_index=True).drop_duplicates()
        base = _best_merge(base, aux)
        base = _standardize_opponent_cols(base)
    return base

def main():
    # 1) Try enriched, but only use if it has rows & identifiers
    use_enriched = False
    if ENRICHED_FILE.exists():
        try:
            enr = pd.read_csv(ENRICHED_FILE)
            if not enr.empty and ("player_id" in enr.columns or "pitcher_id" in enr.columns):
                base = _standardize_opponent_cols(enr)
                use_enriched = True
            else:
                print("NOTE: Enriched file empty or missing ids; falling back to base merge.")
                base = _load_base_with_fallbacks()
        except Exception as e:
            print(f"WARN: Failed to read enriched file: {e}; falling back.")
            base = _load_base_with_fallbacks()
    else:
        base = _load_base_with_fallbacks()

    # Basic sanitation
    for c in ("player_id","team","name","last_name, first_name"):
        if c in base.columns: base[c] = base[c].astype(str).str.strip()
    if "name" not in base.columns and "last_name, first_name" in base.columns:
        base["name"] = base["last_name, first_name"]

    # 2) Project
    if base.empty:
        print("WARN: No pitcher rows to project; writing empty header to flag pipeline.")
        OUT = base.head(0).copy()
        OUT.to_csv(OUTPUT_FILE, index=False)
        print(f"Wrote: {OUTPUT_FILE} (0 rows)")
        return

    df_proj = calculate_all_projections(base)
    df_proj = df_proj.dropna(how="all")
    df_proj = df_proj[df_proj.get("player_id").notna()] if "player_id" in df_proj.columns else df_proj

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_proj.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote: {OUTPUT_FILE} ({len(df_proj)} rows)  source={'enriched' if use_enriched else 'base'}")

if __name__ == "__main__":
    main()
