import pandas as pd
import json
import random

# Simulated data - in real version this would be scraped from Baseball Savant
batters = [
    {"batter": "Pete Alonso", "xwoba": 0.416, "barrel_rate": 20.1, "sweet_spot": 39.8},
    {"batter": "Aaron Judge", "xwoba": 0.472, "barrel_rate": 26.2, "sweet_spot": 39.1},
    {"batter": "Juan Soto", "xwoba": 0.458, "barrel_rate": 17.5, "sweet_spot": 34.3},
    {"batter": "James Wood", "xwoba": 0.411, "barrel_rate": 19.2, "sweet_spot": 34.9},
    {"batter": "Oneil Cruz", "xwoba": 0.358, "barrel_rate": 22.0, "sweet_spot": 35.6},
    {"batter": "Kyle Stowers", "xwoba": 0.381, "barrel_rate": 19.5, "sweet_spot": 37.9},
    {"batter": "Austin Riley", "xwoba": 0.343, "barrel_rate": 14.5, "sweet_spot": 37.1},
    {"batter": "Rafael Devers", "xwoba": 0.386, "barrel_rate": 15.5, "sweet_spot": 36.1},
    {"batter": "Matt Olson", "xwoba": 0.390, "barrel_rate": 17.0, "sweet_spot": 39.0},
    {"batter": "Shohei Ohtani", "xwoba": 0.431, "barrel_rate": 22.0, "sweet_spot": 32.8}
]

pitchers = ["Tylor Megill", "Kyle Freeland", "Quinn Priester", "Nick Lodolo", "MacKenzie Gore",
            "Luis Severino", "Paul Skenes", "Chris Sale", "Shane Smith", "Grant Holmes"]

# Assign random pitcher to each batter and calculate edge score
for b in batters:
    b["pitcher"] = random.choice(pitchers)
    b["edge_score"] = round(
        (b["xwoba"] + (b["barrel_rate"] / 100) + (b["sweet_spot"] / 100)) * 10, 2
    )

# Output to JSON
with open("top_props.json", "w") as f:
    json.dump(batters, f, indent=2)

print("top_props.json generated.")