import requests
from logging_utils import log
from config import API_KEY, PLAYER_PROPS_ENDPOINT, GAME_PROPS_ENDPOINT

def _safe_get(url: str, headers: dict | None = None):
    # Return JSON or []; never raise. Skip placeholders.
    if not url or "example.com" in url or not url.startswith(("http://","https://")):
        log(f"Skipping fetch (endpoint not configured): {url!r}")
        return []
    try:
        r = requests.get(url, headers=headers or {}, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        log(f"Fetch failed for {url}: {e}. Returning empty.")
        return []

def fetch_player_props():
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    return _safe_get(PLAYER_PROPS_ENDPOINT, headers)

def fetch_game_props():
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    return _safe_get(GAME_PROPS_ENDPOINT, headers)
