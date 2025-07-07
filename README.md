# 🧠 Sports Betting Site (MLB Edition)

A fully automated MLB betting insights generator using live data, park/weather conditions, and advanced metrics. This system generates optimized 5-pick parlays, top home run hitters, and hit props daily using **real data**, not placeholders.

## 🔧 Features

- ✅ Auto-generated daily MLB betting props
- ⚾️ Stadium-aware weather effects (wind, temperature, dome logic)
- 📊 Park factors integrated from Baseball Savant
- 💡 Sharp player prop predictions (xwOBA, barrel%, sweet spot%)
- 🔁 Fully scheduled GitHub Actions (runs daily at 7:00 AM ET)
- 🌐 Deployed via GitHub Pages: [Live Site](https://clownworldenjoyer76.github.io/sports-betting-site/)

---

## 📂 Repo Structure

```
sports-betting-site/
│
├── .github/workflows/
│   ├── daily_bets.yml           # Generates daily_bets.json
│   └── scrape_savant.yml        # Generates top_props.json
│
├── data/
│   └── Data/
│       ├── park_factors_full_verified.csv
│       └── team_name_map.py
│
├── generate_daily_bets.py       # Main game + weather + park logic
├── scrape_savant.py             # Scrapes player props & applies boosts
├── top_props.json               # Auto-generated daily player props
├── daily_bets.json              # Auto-generated game analysis
├── index.html                   # Loads and displays JSON on site
├── style.css                    # Simple styling (optional)
└── README.md                    # You're reading it
```

---

## 🚀 Setup (Local Testing)

If you want to run manually:

```bash
pip install pandas requests beautifulsoup4 lxml
python generate_daily_bets.py
python scrape_savant.py
```

---

## 📈 Output

### `daily_bets.json`
Contains:
- Matchup info
- Park factors
- Live weather data
- Placeholder for SGP picks

### `top_props.json`
Contains:
- Top 10 batters based on xwOBA, barrel%, sweet spot%
- Adjusted using real-time weather and park impact

---

## 🧠 Credits

- MLB Stats API
- Baseball Savant
- OpenWeatherMap API

---

## 📬 Contact

For suggestions, bugs, or collabs, open an issue or PR.

---