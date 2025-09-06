import subprocess

def main():
    files = ["index.html", "data/output/top_picks.json"]
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", "Auto-update index.html and top_picks.json"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed index.html and top_picks.json to repo.")
    except subprocess.CalledProcessError as e:
        print("⚠️ No changes to commit or push.")

if __name__ == "__main__":
    main()
