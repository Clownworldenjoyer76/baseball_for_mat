# scripts/config.py
DATA_DIR = "data/bets"
PLAYER_HISTORY_FILE = f"{DATA_DIR}/player_props_history.csv"
GAME_HISTORY_FILE   = f"{DATA_DIR}/game_props_history.csv"

# Schemas (adjust names as you finalize)
PLAYER_HISTORY_COLUMNS = [
    "date","book","sport","league","game_id","player","prop","side","line","price","result","timestamp"
]
GAME_HISTORY_COLUMNS = [
    "date","book","sport","league","game_id","market","selection","line","price","result","timestamp"
]

# API placeholders (replace)
API_KEY = ""
PLAYER_PROPS_ENDPOINT = ""   # e.g. https://api.yourbook.com/player-props
GAME_PROPS_ENDPOINT   = ""   # e.g. https://api.yourbook.com/game-props
