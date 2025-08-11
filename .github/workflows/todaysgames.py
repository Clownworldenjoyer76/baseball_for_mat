name: Update Todays Games CSV

on:
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install requests

      - name: Run script to create todaysgames.csv
        run: |
          python scripts/todaysgames.py --out todaysgames.csv

      - name: Commit and push CSV
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@github.com"
          git add todaysgames.csv
          git diff --quiet && git diff --staged --quiet || git commit -m "Update todaysgames.csv"
          git push
