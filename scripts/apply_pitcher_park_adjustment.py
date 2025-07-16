name: Apply Pitcher Park Adjustment

on:
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: pip install pandas

      - name: Run pitcher park adjustment
        run: python scripts/apply_pitcher_park_adjustment.py

      - name: Upload adjusted pitcher files
        uses: actions/upload-artifact@v4
        with:
          name: adjusted-pitchers
          path: |
            data/adjusted/pitchers_*_park.csv
            log_pitchers_*_park.txt
