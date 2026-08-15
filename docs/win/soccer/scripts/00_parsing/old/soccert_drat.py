import re
import time
import random
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

ERROR_DIR = Path("docs/win/soccer/errors/00_parsing")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE  = ERROR_DIR / "soccert_drat_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== soccert_drat RUN {datetime.now(timezone.utc).isoformat()} ===\n")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()} | {msg}\n")

URLS = {
    "mls":        "https://www.dratings.com/predictor/mls-soccer-predictions/",
    "epl":        "https://www.dratings.com/predictor/english-premier-league-predictions/",
    "ligue1":     "https://www.dratings.com/predictor/france-ligue-1-predictions/",
    "laliga":     "https://www.dratings.com/predictor/spain-la-liga-predictions/",
    "bundesliga": "https://www.dratings.com/predictor/german-bundesliga-predictions/",
    "seriea":     "https://www.dratings.com/predictor/italy-serie-a-predictions/",
}

LEAGUE_MAP = {
    "mls":        "MLS",
    "epl":        "EPL",
    "ligue1":     "LIGUE1",
    "laliga":     "LALIGA",
    "bundesliga": "BUNDESLIGA",
    "seriea":     "SERIEA",
}

MLS_RECORD_PAT = re.compile(r"\s*\(\d+-\d+-\d+\)\s*$")

def strip_record(name: str) -> str:
    return MLS_RECORD_PAT.sub("", name).strip()

def scrape_page(page, url):
    page.goto(url)
    page.wait_for_selector("table")
    rows = page.query_selector_all("table tbody tr")
    return [[c.inner_text().strip() for c in r.query_selector_all("td")] for r in rows]

def split_date_time(dt):
    parts = dt.replace("\n", " ").split()
    date  = datetime.strptime(parts[0], "%m/%d/%Y").strftime("%Y_%m_%d")
    time_ = " ".join(parts[1:])
    return date, time_

def safe_split(val):
    return val.split("\n") if "\n" in val else val.split()

def is_score_cell(val):
    parts = safe_split(val)
    return len(parts) == 2 and all(p.isdigit() for p in parts)

def parse_prediction(row, league):
    try:
        date, time_ = split_date_time(row[0])
        teams     = safe_split(row[1])
        away_team = strip_record(teams[0])
        home_team = strip_record(teams[1])
        probs     = safe_split(row[2])
        away_prob = probs[0]
        home_prob = probs[1]
        draw_prob = row[3]
        xg        = safe_split(row[5]) if len(row) > 5 else ["", ""]
        away_xg   = xg[0] if len(xg) > 0 else ""
        home_xg   = xg[1] if len(xg) > 1 else ""
        total     = row[6] if len(row) > 6 else ""

        return {
            "sport":                "soccer",
            "league":               league,
            "market":               "",
            "match_date":           date,
            "match_time":           time_,
            "home_team":            home_team,
            "away_team":            away_team,
            "home_prob":            home_prob,
            "draw_prob":            draw_prob,
            "away_prob":            away_prob,
            "home_xg":              home_xg,
            "away_xg":              away_xg,
            "expected_total_goals": total,
        }
    except Exception as e:
        log(f"PREDICTION PARSE ERROR {league} | {row} | {e}")
        return None

def parse_final(row, league):
    try:
        date, time_ = split_date_time(row[0])
        teams     = safe_split(row[1])
        away_team = strip_record(teams[0])
        home_team = strip_record(teams[1])

        score_idx = None
        for i, cell in enumerate(row):
            if is_score_cell(cell):
                score_idx = i
                break

        if score_idx is None:
            log(f"NO SCORE FOUND {league} | {row}")
            return None

        score      = safe_split(row[score_idx])
        away_score = score[0]
        home_score = score[1]

        return {
            "sport":      "soccer",
            "league":     league,
            "market":     "",
            "game_date":  date,
            "match_time": time_,
            "home_team":  home_team,
            "away_team":  away_team,
            "home_score": home_score,
            "away_score": away_score,
        }
    except Exception as e:
        log(f"FINAL PARSE ERROR {league} | {row} | {e}")
        return None

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()

        today = datetime.now(timezone.utc).strftime("%Y_%m_%d")

        for key, url in URLS.items():
            league = LEAGUE_MAP[key]
            log(f"Scraping {league}")
            try:
                raw = scrape_page(page, url)

                predictions = []
                finals      = []

                for r in raw:
                    if not r or "Sportsbooks" in r[0] or "DRatings" in r[0]:
                        continue
                    has_score = any(is_score_cell(c) for c in r)
                    if has_score:
                        parsed = parse_final(r, league)
                        if parsed:
                            finals.append(parsed)
                    else:
                        parsed = parse_prediction(r, league)
                        if parsed:
                            predictions.append(parsed)

                log(f"{league} → predictions: {len(predictions)}, finals: {len(finals)}")
                print(f"\n--- {league} → predictions: {len(predictions)}, finals: {len(finals)} ---")

                # ── Write one prediction file per match_date ──────────────
                if predictions:
                    pred_df = pd.DataFrame(predictions)
                    for match_date, group in pred_df.groupby("match_date"):
                        pred_path = Path(f"docs/win/soccer/00_intake/predictions/{league}/{match_date}_{league}.csv")
                        pred_path.parent.mkdir(parents=True, exist_ok=True)
                        group.to_csv(pred_path, index=False)
                        log(f"WROTE predictions → {pred_path} ({len(group)} rows)")

                # ── Write finals grouped by game_date ─────────────────────
                if finals:
                    final_df = pd.DataFrame(finals)
                    for game_date, group in final_df.groupby("game_date"):
                        final_path = Path(f"docs/win/final_scores/results/soccer/final_scores/{league}/{game_date}_{league}.csv")
                        final_path.parent.mkdir(parents=True, exist_ok=True)
                        group.to_csv(final_path, index=False)
                        log(f"WROTE finals → {final_path} ({len(group)} rows)")

            except Exception as e:
                log(f"ERROR scraping {league}: {e}\n{traceback.format_exc()}")

            time.sleep(random.uniform(2, 4))

        browser.close()

    log("COMPLETE")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"FATAL:\n{e}\n{traceback.format_exc()}")
        raise
