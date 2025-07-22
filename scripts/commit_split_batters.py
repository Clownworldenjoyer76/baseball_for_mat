import subprocess
from datetime import datetime

timestamp = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p %Z")

files_to_commit = [
    "data/adjusted/batters_home.csv",
    "data/adjusted/batters_away.csv"
]

try:
    subprocess.run(["git", "add"] + files_to_commit, check=True)
    commit_msg = f"🔄 Split batters home/away @ {timestamp}"
    subprocess.run(["git", "commit", "-m", commit_msg], check=True)
    subprocess.run(["git", "push"], check=True)
    print(f"✅ Git commit and push successful: {commit_msg}")
except subprocess.CalledProcessError as e:
    print(f"❌ Git commit/push failed: {e}")
