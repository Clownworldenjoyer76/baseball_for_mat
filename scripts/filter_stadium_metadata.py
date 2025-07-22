import pandas as pd
import os
from datetime import datetime

STADIUM_FILE = "data/Data/stadium_metadata.csv"
LOG_DIR = "summaries"
os.makedirs(LOG_DIR, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join(LOG_DIR, f"log_filter_stadium_metadata_{timestamp}.txt")

def filter_stadium_metadata():
    df = pd.read_csv(STADIUM_FILE)
    original_rows = len(df)
    
    # Remove rows where away_team is blank or NaN
    df_cleaned = df[df["away_team"].notna() & (df["away_team"].str.strip() != "")]
    cleaned_rows = len(df_cleaned)
    removed = original_rows - cleaned_rows

    # Save cleaned file
    df_cleaned.to_csv(STADIUM_FILE, index=False)

    # Log results
    with open(log_file, "w") as f:
        f.write(f"✅ filter_stadium_metadata.py executed at {timestamp}\n")
        f.write(f"Original rows: {original_rows}\n")
        f.write(f"Removed rows: {removed}\n")
        f.write(f"Remaining rows: {cleaned_rows}\n")

    # Git commit and push
    os.system("git add data/Data/stadium_metadata.csv")
    os.system(f'git commit -m "🧹 Removed empty away_team rows from stadium_metadata.csv ({removed} rows removed)" || echo "Nothing to commit"')
    os.system("git add summaries/")
    os.system(f'git commit -m "📝 Log: filter_stadium_metadata.py at {timestamp}" || echo "Nothing to commit"')
    os.system("git push || echo 'Nothing to push'")

if __name__ == "__main__":
    filter_stadium_metadata()
