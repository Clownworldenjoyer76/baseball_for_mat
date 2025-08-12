# fetch_data.py
import requests
from config import API_KEY, PLAYER_PROPS_ENDPOINT, GAME_PROPS_ENDPOINT

def fetch_player_props():
    # Replace with actual API logic
    response = requests.get(PLAYER_PROPS_ENDPOINT, headers={"Authorization": f"Bearer {API_KEY}"})
    response.raise_for_status()
    return response.json()

def fetch_game_props():
    response = requests.get(GAME_PROPS_ENDPOINT, headers={"Authorization": f"Bearer {API_KEY}"})
    response.raise_for_status()
    return response.json()
