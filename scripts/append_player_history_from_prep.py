# scripts/append_player_history_from_prep.py
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


PREP_FILE = Path("data/bets/prep/batter_props_final.csv")
PROJ_FILE = Path("data/_projections/batter_props_projected.csv")
OUT_FILE  = Path("data/bets/player_props_history.csv")

# Mapping from (prop, line) -> projection column
PROP_PROJ_COL = {
    ("hits", 1.5): "prob_hits_over_1p5",
    ("total_bases", 1.5): "prob_tb_over_1p5",
    ("hr", 0.5): "prob_hr_over_0p5",
}


def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _read_csv_safe(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists():
        return None
    try:
        return _std(pd.read_csv(p))
    except Exception:
        return None


def _normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Lightweight ID normalization to keep types consistent across runs."""
    df = df.copy()

    # player_id → Int64 (nullable)
    if "player_id" in df.columns:
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")

    # game_id → string (nullable), strip trailing .0 if present
    if "game_id" in df.columns:
        gid = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
        df["game_id"] = gid.astype("string")

    # line → float
    if "line" in df.columns:
        df["line"] = pd.to_numeric(df["line"], errors="coerce")

    # prop lowercase & trimmed
    if "prop" in df.columns:
        df["prop"] = df["prop"].astype(str).strip().str.lower()

    # date → yyyy-mm-dd as string
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["date"] = dt.astype("string")

    return df


def _choose_proj_col(prop: str, line: float) -> Optional[str]:
    key = (prop, float(line) if pd.notna(line) else None)
    return PROP_PROJ_COL.get(key)


def _merge_probs(prep: pd.DataFrame, proj: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge in model probabilities from projections when available.
    IMPORTANT: No clipping or jitter is applied to over_probability.
    """
    df = prep.copy()

    # Ensure over_probability is numeric if present (no clipping)
    if "over_probability" in df.columns:
        df["over_probability"] = pd.to_numeric(df["over_probability"], errors="coerce")

    if proj is None or proj.empty:
        return df

    proj = _std(proj)

    # Normalize IDs in projections too
    if "player_id" in proj.columns:
        proj["player_id"] = pd.to_numeric(proj["player_id"], errors="coerce").astype("Int64")

    # Bring all prob_* columns alongside player_id (m:1)
    join_cols = [c for c in ["player_id"] if c in df.columns and c in proj.columns]
    if join_cols:
        keep_cols = ["player_id"] + [c for c in proj.columns if c.startswith("prob_")]
        merged = df.merge(
            proj[keep_cols].drop_duplicates("player_id"),
            on="player_id",
            how="left",
            validate="m:1",
        )
    else:
        merged = df.copy()

    # Row-select the correct upstream projection column when present
    def row_prob(row):
        col = _choose_proj_col(row.get("prop"), row.get("line"))
        if col and col in merged.columns:
            return row.get(col)
        return pd.NA

    merged["__proj_prob"] = merged.apply(row_prob, axis=1)

    # Prefer projection probability when present, else keep existing value (no clipping)
    if "over_probability" in merged.columns:
        merged["over_probability"] = pd.to_numeric(merged["over_probability"], errors="coerce")

    merged["over_probability"] = merged["__proj_prob"].where(
        merged["__proj_prob"].notna(),
        merged.get("over_probability", pd.NA),
    )

    merged.drop(columns=[c for c in ["__proj_prob"] if c in merged.columns], inplace=True)
    return merged


def _ensure_prop_sort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make prop_sort a pure probability-based sort key:
    - Higher over_probability should come first.
    - Missing probs go to the bottom.
    - Deterministic micro-jitter ONLY in the sort key to stabilize ties.
      (Does NOT modify over_probability itself.)
    """
    df = df.copy()

    p = pd.to_numeric(df.get("over_probability"), errors="coerce")

    # base sort key: negative prob so ascending => highest prob first
    sort_key = -p

    # small deterministic tie-breaker (~1e-6 range)
    def _tie_key(row):
        seed = f"{row.get('player_id')}-{row.get('prop')}-{row.get('line')}"
        h = int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)
        return (h % 10_000) / 10_000_000.0  # 0 .. 0.001

    jitter = df.apply(_tie_key, axis=1)

    # if prob is NaN, push far down
    sort_key = sort_key.where(p.notna(), 1e9) + jitter

    df["prop_sort"] = sort_key.astype("float64")
    return df


def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Drop rows with missing game_id only if the column exists
    if "game_id" in df.columns:
        df = df[df["game_id"].notna()].copy()

    # Preferred de-dup key (include game_id if present)
    preferred = ["date", "game_id", "player_id", "team", "prop", "line"]
    keys = [k for k in preferred if k in df.columns]
    if not keys:
        return df

    # Keep the latest record when multiple exist
    order_cols = [c for c in ("timestamp", "asof", "created_at", "date") if c in df.columns]
    if order_cols:
        df = df.sort_values(by=order_cols)

    return df.drop_duplicates(subset=keys, keep="last")


def _ensure_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a 'timestamp' column exists, filling missing with current UTC ISO8601.
    (We use Z-suffixed UTC for consistency across runners.)
    """
    df = df.copy()
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if "timestamp" not in df.columns:
        df["timestamp"] = now_iso
    else:
        # only fill missing/null
        df["timestamp"] = df["timestamp"].fillna(now_iso)
    return df


def main():
    prep = _read_csv_safe(PREP_FILE)
    if prep is None or prep.empty:
        print(f"⚠️ {PREP_FILE} is missing or empty; nothing to append.")
        return

    prep = _normalize_ids(prep)
    proj = _read_csv_safe(PROJ_FILE)

    # Merge/repair probabilities (NO clipping)
    prep = _merge_probs(prep, proj)

    # Ensure timestamp exists (add current UTC if missing)
    prep = _ensure_timestamp(prep)

    # Probability-based prop_sort (higher prob first; tie-stable)
    prep = _ensure_prop_sort(prep)

    # Keep only known columns first, then preserve any extras
    base_cols = [
        "player_id", "name", "team", "prop", "line", "value",
        "over_probability", "date", "game_id", "prop_sort", "timestamp"
    ]
    cols = [c for c in base_cols if c in prep.columns] + [c for c in prep.columns if c not in base_cols]
    prep = prep[cols]

    # De-duplicate within today's prep
    prep = _dedupe(prep)

    # Append to history
    if OUT_FILE.exists():
        hist = _read_csv_safe(OUT_FILE)
        if hist is None:
            hist = pd.DataFrame(columns=prep.columns)
    else:
        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        hist = pd.DataFrame(columns=prep.columns)

    combo = pd.concat([hist, prep], ignore_index=True, sort=False)
    combo = _normalize_ids(combo)
    combo = _ensure_timestamp(combo)
    combo = _ensure_prop_sort(combo)
    combo = _dedupe(combo)

    combo.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(combo)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
