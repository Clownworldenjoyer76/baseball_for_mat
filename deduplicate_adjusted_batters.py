
import pandas as pd
from pathlib import Path

def main():
    input_file = "data/adjusted/batters_adjusted_weather_park.csv"
    output_file = "data/adjusted/batters_deduped.csv"
    log_file = "data/adjusted/deduplication_log.txt"

    df = pd.read_csv(input_file)
    before = len(df)

    if 'player_id' not in df.columns:
        raise ValueError("Missing 'player_id' column — cannot deduplicate.")

    # Drop duplicates based on player_id
    df = df.drop_duplicates(subset='player_id', keep='first')
    after = len(df)

    df.to_csv(output_file, index=False)

    with open(log_file, "w") as log:
        log.write(f"✅ Deduplicated file written to {output_file}\n")
        log.write(f"Original rows: {before}\n")
        log.write(f"Remaining rows: {after}\n")
        log.write(f"Removed: {before - after}\n")

    print(f"✅ Done: {before - after} duplicates removed. Output saved to {output_file}")

if __name__ == "__main__":
    main()
