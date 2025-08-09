name: Score Bets

on:
  workflow_dispatch:

permissions:
  contents: write

jobs:
  score-bets:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          ref: main

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.10"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run score_game_bets_range.py
        run: python scripts/score_game_bets_range.py

      - name: Run score_player_bets.py
        run: python scripts/score_player_bets.py

      - name: Commit and push results
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@github.com"
          git add data/bets/*.csv
          git diff --quiet && git diff --staged --quiet || git commit -m "Update bet scoring results"
          git push
