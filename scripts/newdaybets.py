#!/usr/bin/env python3
import shutil
from pathlib import Path

# Define source and destination paths
files_to_copy = [
    ("data/bets/newdaycsvs/game_props_history.csv", "data/bets/game_props_history.csv"),
    ("data/bets/newdaycsvs/player_props_history.csv", "data/bets/player_props_history.csv"),
]

for src, dst in files_to_copy:
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        print(f"❌ Source file not found: {src_path}")
        continue

    try:
        shutil.copy2(src_path, dst_path)
        print(f"✅ Copied {src_path} → {dst_path}")
    except Exception as e:
        print(f"❌ Error copying {src_path} to {dst_path}: {e}")
