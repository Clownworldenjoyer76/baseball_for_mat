#!/usr/bin/env python3
# Reads the projected batter props and reshapes for the builder.
import pandas as pd
from pathlib import Path

IN  = Path("data/_projections/batter_props_projected.csv")
OUT = Path("data/_projections/batter_props_expanded.csv")

def main():
    if not IN.exists():
        raise SystemExit(f"❌ Missing input: {IN}")
    z = pd.read_csv(IN)

    # (example passthrough; keep columns needed downstream)
    keep = [c for c in ["player_id","name","team","game_id","date",
                        "prob_hits_over_1p5","prob_tb_over_1p5","prob_hr_over_0p5",
                        "proj_pa_used","proj_ab_est","proj_avg_used","proj_iso_used"] if c in z.columns]
    out = z[keep].copy()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"✅ Wrote: {OUT} ({len(out)} rows)")

if __name__ == "__main__":
    main()
