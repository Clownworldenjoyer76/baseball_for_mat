#!/usr/bin/env python3
# scripts/finalize_projections.py
#
# Finalize projection outputs for batters & pitchers.
# Ensures pitcher projections are written to BOTH:
#   - data/_projections/pitcher_props_projected_final.csv
#   - data/end_chain/final/pitcher_props_projected_final.csv
#
# If a pitcher source is missing, we search fallbacks and fail with a clear error
# instead of silently skipping.
#
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

PROJ_DIR  = Path("data/_projections")
FINAL_DIR = Path("data/end_chain/final")
PROJ_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)

# ---- Input candidates (searched in order) ----
PITCHER_SOURCES = [
    PROJ_DIR / "pitcher_props_projected.csv",          # main intermediate from project_pitcher_props.py
    PROJ_DIR / "pitcher_props_projected_final.csv",    # may already exist from a previous run
    FINAL_DIR / "pitcher_props_projected_final.csv",   # previous final can be used as a fallback
]

# ---- Output targets ----
PITCHER_OUT_PROJ  = PROJ_DIR  / "pitcher_props_projected_final.csv"
PITCHER_OUT_FINAL = FINAL_DIR / "pitcher_props_projected_final.csv"

# Batter files are already produced earlier in the pipeline;
# we mirror them into end_chain/final for convenience if present.
BATTER_FILES = [
    ("batter_props_projected_final.csv",  PROJ_DIR / "batter_props_projected_final.csv",  FINAL_DIR / "batter_props_projected_final.csv"),
    ("batter_props_expanded_final.csv",   PROJ_DIR / "batter_props_expanded_final.csv",   FINAL_DIR / "batter_props_expanded_final.csv"),
    ("pitcher_mega_z_final.csv",          PROJ_DIR / "pitcher_mega_z_final.csv",          FINAL_DIR / "pitcher_mega_z_final.csv"),
]

MIN_PITCHER_COLS = ["player_id", "game_id", "team_id", "opponent_team_id", "pa"]  # permissive; we won’t drop extra columns


def _read_csv_str(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df


def _normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    def norm(s):
        s = str(s or "").strip()
        if s.endswith(".0") and s[:-2].isdigit():
            return s[:-2]
        return s if s.lower() not in {"nan", "none"} else ""
    for c in ["player_id", "game_id", "team_id", "opponent_team_id"]:
        if c in df.columns:
            df[c] = df[c].map(norm)
    return df


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _copy_if_exists(label: str, src: Path, dst: Path) -> None:
    if src.exists():
        df = _read_csv_str(src)
        dst.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(dst, index=False)
        print(f"[OK] Mirrored {label}: {src} -> {dst}")


def find_pitcher_source() -> Path | None:
    for p in PITCHER_SOURCES:
        if p.exists() and p.is_file():
            return p
    return None


def finalize_pitchers() -> int:
    src = find_pitcher_source()
    if not src:
        print(
            "ERROR: No pitcher projection source found. Looked for:\n  - "
            + "\n  - ".join(str(p) for p in PITCHER_SOURCES),
            file=sys.stderr,
        )
        return 1

    df = _read_csv_str(src)
    df = _ensure_cols(df, MIN_PITCHER_COLS)
    df = _normalize_ids(df)
    df = _coerce_numeric(df, ["pa"])  # keep numeric where expected; safe no-op if non-numeric

    # Write to BOTH locations
    PITCHER_OUT_PROJ.parent.mkdir(parents=True, exist_ok=True)
    PITCHER_OUT_FINAL.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PITCHER_OUT_PROJ, index=False)
    df.to_csv(PITCHER_OUT_FINAL, index=False)

    print(f"[OK] Pitchers finalized from {src.name}: rows={len(df)}")
    print(f" -> {PITCHER_OUT_PROJ}")
    print(f" -> {PITCHER_OUT_FINAL}")
    return 0


def mirror_batters() -> None:
    for label, src, dst in BATTER_FILES:
        _copy_if_exists(label, src, dst)


def main() -> None:
    # 1) Always try to finalize pitchers (hard requirement for downstream).
    rc = finalize_pitchers()
    # 2) Mirror batter & mega_z convenience files if they exist
    mirror_batters()
    if rc != 0:
        # non-zero so CI step is visibly marked as failed
        sys.exit(rc)
    print("✅ finalize_projections.py completed")


if __name__ == "__main__":
    main()
