"""
This file provides the list of functions needed to manage
the watchlist of tickers. These functions only interact with a local
JSON file, not a database or S3 object and is used purely for the
watchlist of tickers that the user wants to track.
"""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Ensures that the watchlist file is stored in the same directory
# as this script, making it easy to find and manage.
WATCHLIST_FILE = Path(__file__).parent / "watchlist.json"

"""
This helper loads the watchlist from local storage and allows the user
to add or remove tickers from the watchlist.
"""
def load_watchlist() -> list[str]:
    # If the watchlist file doesn't exist, return an empty list.
    if not WATCHLIST_FILE.exists():
        return []

    # Opens the file and loads the JSON data, returning the list of tickers.
    with open(WATCHLIST_FILE, "r") as f:
        data = json.load(f)

    # Gets the list of tickers from JSON data
    return data.get("tickers", [])

"""
This function saves the watchlist to local storage through
overwriting the watchlist file with the given list of tickers.
"""
def save_watchlist(tickers: list[str]) -> None:
    with open(WATCHLIST_FILE, "w") as f:
        json.dump({"tickers": tickers}, f, indent=2)

"""
This function adds one or more tickers to the watchlist and returns
the updated list of tickers in the watchlist.
"""
def add_tickers(tickers: list[str]) -> list[str]:
    # Standardize the tickers to uppercase and remove any whitespace
    normalized = [t.strip().upper() for t in tickers if t.strip()]
    # Load the current watchlist from local storage
    current = load_watchlist()

    # Add new tickers to the watchlist if they are not already present
    added = []
    for ticker in normalized:
        if ticker not in current:
            current.append(ticker)
            added.append(ticker)

    # If any new tickers were added, save the updated watchlist to local storage
    if added:
        save_watchlist(current)
        logger.info(f"Added {added} to watchlist")
    else:
        logger.info("No new tickers added (already present or empty input)")
    return current, added

"""
This function removes one or more tickers from the watchlist and
returns the updated list of tickers in the watchlist.
"""
def remove_tickers(tickers: list[str]) -> list[str]:
    # Standardize the tickers to uppercase and remove any whitespace
    normalized = [t.strip().upper() for t in tickers if t.strip()]
    # Loads the current watchlist from local storage
    current = load_watchlist()

    # Removes the specified tickers from the watchlist if they are present
    removed = []
    for ticker in normalized:
        if ticker in current:
            current.remove(ticker)
            removed.append(ticker)

    # If any tickers were removed, save the updated watchlist to local storage
    if removed:
        save_watchlist(current)
        logger.info(f"Removed {removed} from watchlist")
    else:
        logger.info("No tickers removed (none matched)")
    return current, removed

"""
These functions are currently not used since the API endpoints are designed to
handle lists of tickers to begin with, but they are provided for convenience if needed in the future.

def add_ticker(ticker: str) -> list[str]:
    return add_tickers([ticker])


def remove_ticker(ticker: str) -> list[str]:
    return remove_tickers([ticker])
"""