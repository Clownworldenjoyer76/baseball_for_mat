import pandas as pd
import subprocess

HOME_FILE = "data/adjusted/pitchers_home.csv"
AWAY_FILE = "data/adjusted/pitchers_away.csv"
LOG_FILE = "log_add_team_alias_columns.txt"

def add_alias_column(file_path, new_col):
    try:
        df = pd.read_csv(file_path)
        if "team" not in df.columns:
            return f"❌ 'team' column missing in {file_path}"
        df[new_col] = df["team"]
        df.to_csv(file_path, index=False)
        return f"✅ Added '{new_col}' to {file_path} ({len(df)} rows)"
    except Exception as e:
        return f"❌ Error processing {file_path}: {e}"

def git_commit_and_push(files):
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", "Add team alias columns to pitcher files"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git push failed: {e}")

def main():
    logs = []
    logs.append(add_alias_column(HOME_FILE, "home_team"))
    logs.append(add_alias_column(AWAY_FILE, "away_team"))

    with open(LOG_FILE, "w") as f:
        f.write("\n".join(logs))
    print("\n".join(logs))
    print(f"📝 Log saved to {LOG_FILE}")

    git_commit_and_push([HOME_FILE, AWAY_FILE, LOG_FILE])

if __name__ == "__main__":
    main()
