import pandas as pd
from pydantic import BaseModel, ValidationError
from typing import Optional
from tqdm import tqdm
import sys

# ─── Pydantic Models ─────────────────────────────────────────────

class BatterRow(BaseModel):
    name: str
    player_id: int
    team: str
    type: str
    ab: int
    pa: int
    hit: int
    single: int
    double: int
    triple: int
    home_run: int
    strikeout: int
    walk: int
    k_percent: float
    bb_percent: float
    batting_avg: float
    slg_percent: float
    on_base_percent: float
    on_base_plus_slg: float
    xba: float
    xslg: float
    woba: float
    xwoba: float
    xobp: float
    xiso: float
    avg_swing_speed: float
    fast_swing_rate: float
    blasts_contact: float
    blasts_swing: float
    squared_up_contact: float
    squared_up_swing: float
    avg_swing_length: float
    swords: float
    attack_angle: float
    attack_direction: float
    ideal_angle_rate: float
    vertical_swing_path: float
    exit_velocity_avg: float
    launch_angle_avg: float
    sweet_spot_percent: float
    barrel_batted_rate: float
    hard_hit_percent: float
    avg_best_speed: float
    avg_hyper_speed: float
    whiff_percent: float
    swing_percent: float

class PitcherRow(BaseModel):
    name: str
    player_id: int
    team: str
    type: str
    year: int
    player_age: int
    p_game: int
    p_formatted_ip: str
    pa: int
    ab: int
    hit: int
    single: int
    double: int
    triple: int
    home_run: int
    strikeout: int
    walk: int
    k_percent: float
    bb_percent: float
    batting_avg: float
    slg_percent: float
    on_base_percent: float
    on_base_plus_slg: float
    xba: float
    xslg: float
    woba: float
    xwoba: float
    xobp: float
    xiso: float
    avg_swing_speed: float
    fast_swing_rate: float
    blasts_contact: float
    blasts_swing: float
    squared_up_contact: float
    squared_up_swing: float
    avg_swing_length: float
    swords: float
    attack_angle: float
    attack_direction: float
    ideal_angle_rate: float
    vertical_swing_path: float
    exit_velocity_avg: float
    launch_angle_avg: float
    sweet_spot_percent: float
    barrel_batted_rate: float
    hard_hit_percent: float
    avg_best_speed: float
    avg_hyper_speed: float
    whiff_percent: float
    swing_percent: float

# ─── Validation Routine ─────────────────────────────────────────

def validate_file(file_path: str, model_class: BaseModel, label: str):
    print(f"\n🔍 Validating: {file_path}")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Failed to load {label} file: {e}")
        sys.exit(1)

    failed_rows = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Validating {label}"):
        try:
            model_class(**row.to_dict())
        except ValidationError as e:
            failed_rows.append((idx, e))

    if failed_rows:
        print(f"\n❌ {len(failed_rows)} {label} row(s) failed validation:\n")
        for idx, error in failed_rows[:10]:
            print(f"{label} Row {idx}:\n{error}\n")
        if len(failed_rows) > 10:
            print(f"... and {len(failed_rows) - 10} more.")
        sys.exit(1)
    else:
        print(f"✅ All {label} rows passed schema validation.")

# ─── Entry Point ────────────────────────────────────────────────

def main():
    validate_file("data/tagged/batters_normalized.csv", BatterRow, "batters")
    validate_file("data/tagged/pitchers_normalized.csv", PitcherRow, "pitchers")

if __name__ == "__main__":
    main()
