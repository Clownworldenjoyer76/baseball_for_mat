# ⚾ Player Data Pipeline: Full Process

This README outlines the full process for preparing and tagging MLB player data using your GitHub Actions pipeline. It covers both the **Build Player Master** process and the **Player Upload & Tagging Flow**.

---

## [1] Build Player Master Process (`player_team_master.csv`)

### ✅ Purpose:
Builds a master lookup of all players (batters and pitchers), combining name, team, and type into a single CSV for use in tagging and matching.

---

### 📁 1. Upload Files
Upload your raw player files to:

```
data/team_csvs/
```

This folder should contain:
- `batters_[TeamName].csv` (e.g., `batters_Yankees.csv`)
- `pitchers_[TeamName].csv` (e.g., `pitchers_Dodgers.csv`)

Each file should include:
- **Batters:** a column named `name`
- **Pitchers:** a column named `last_name, first_name`

---

### ⚙️ 2. Run the Action
Manually trigger the GitHub Action:

```
Build Player Team Master
```

This runs:  
`build_player_team_master.py`

---

### 🔄 3. What It Does
The script:
- Reads all `batters_*.csv` and `pitchers_*.csv` in `data/team_csvs/`
- Extracts names and team
- Adds a `type` column (`batter` or `pitcher`)
- Normalizes player names:
  - Removes accents
  - Strips punctuation (except commas)
  - Capitalizes both last and first names
- Sorts the combined data
- Outputs to:

```
data/processed/player_team_master.csv
```

---

## [2] Player Data Upload & Processing Flow

---

### 🔹 Step 1: Upload Raw CSVs
Download the latest player data from:

```
https://baseballsavant.mlb.com/
```

Then upload to:

```
data/Data/batters.csv  
data/Data/pitchers.csv
```

---

### 🔹 Step 2: Run Normalization Action

**Purpose:**  
Cleans player names by:
- Removing punctuation
- Stripping accents
- Capitalizing names in `Lastname, Firstname` format

**Reads from:**
```
data/Data/batters.csv  
data/Data/pitchers.csv
```

**Writes to:**
```
data/normalized/batters_normalized.csv  
data/normalized/pitchers_normalized.csv
```

**Action to run:**  
`Normalize Batter and Pitcher Files`

---

### 🔹 Step 3: Run Tag Master Files Action

**Purpose:**  
Tags each player with team/type info by matching against the master lookup file.

**Reads from:**
```
data/normalized/batters_normalized.csv  
data/normalized/pitchers_normalized.csv  
data/processed/player_team_master.csv
```

**Writes to:**
```
data/tagged/batters_normalized.csv  
data/tagged/pitchers_normalized.csv  
data/output/player_totals.txt
```

**Action to run:**  
`Tag Uploaded Master Files`

---

### 🔹 Step 4: Run Deduplication Action

**Purpose:**  
Removes duplicate player entries based on:
- `last_name, first_name`
- `team`
- `type`

**Reads from:**
```
data/tagged/batters_normalized.csv  
data/tagged/pitchers_normalized.csv
```

**Writes to:**
```
data/cleaned/batters_tagged_cleaned.csv  
data/cleaned/pitchers_tagged_cleaned.csv
```

**Action to run:**  
`Duplicate Check After Normalization`
