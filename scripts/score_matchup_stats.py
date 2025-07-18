import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

# All columns your downstream processes depend on
REQUIRED_COLUMNS = [
    "type",
    "pick",
    "adj_woba_combined",
    "player_id_weather",
    "player_id_park",
    "name",
    "team"
]

def inject_missing_columns(df, required_columns):
    for col in required_columns:
        if col not in df.columns:
            print(f"Injecting missing column: {col}")
            if col == "type":
                df[col] = "undecided"
            elif col == "pick":
                df[col] = "TBD"
            elif col == "adj_woba_combined":
                df[col] = 100  # Default neutral wOBA
            else:
                df[col] = "unknown"
    return df

def main():
    df = pd.read_csv(INPUT_FILE)
    df = inject_missing_columns(df, REQUIRED_COLUMNS)

    # Scoring logic goes here (if any)
    # For now, it just passes data through

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
