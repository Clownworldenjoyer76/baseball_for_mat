import pandas as pd
import os

def fix_name_format(name: str) -> str:
    name = name.strip().rstrip(",")
    if "," in name:
        parts = [part.strip() for part in name.split(",")]
        if len(parts) == 2:
            first, last = parts
            return f"{last}, {first}" if " " in first and len(first.split()) == 1 else f"{first}, {last}"
        return name
    elif " " in name:
        parts = name.split()
        if len(parts) == 2:
            return f"{parts[1]}, {parts[0]}"
    return name

def clean_and_deduplicate(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)

    if 'last_name, first_name' in df.columns:
        df['last_name, first_name'] = (
            df['last_name, first_name']
            .astype(str)
            .apply(fix_name_format)
        )
        before = len(df)
        df = df.drop_duplicates(subset='last_name, first_name', keep='first')
        after = len(df)
        print(f"🔍 Removed {before - after} duplicate rows in {filepath}")
    else:
        print(f"⚠️ WARNING: 'last_name, first_name' column not found in {filepath}")

    return df

if __name__ == "__main__":
    input_files = {
        "pitchers_home": "data/end_chain/pitchers_home_weather_park.csv",
        "pitchers_away": "data/end_chain/pitchers_away_weather_park.csv"
    }

    for label, path in input_files.items():
        if os.path.exists(path):
            print(f"Cleaning file: {path}")
            cleaned_df = clean_and_deduplicate(path)
            cleaned_df.to_csv(path, index=False)
            print(f"✅ {label} cleaned and saved.")
        else:
            print(f"❌ File not found: {path}")
