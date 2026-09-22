"""
Background scheduler that periodically refreshes watchlist tickers'
data in S3, while the app is running.
"""
import logging
import time

import s3_data
import watchlist
from dataIngest import fetch_all

logger = logging.getLogger(__name__)

"""
Runs forever, refreshing every watchlist ticker's S3 data once per
`interval_seconds`. Meant to be started on a background thread by
the caller (app.py).
"""
def refresh_loop(bucket_name: str, interval_seconds: int) -> None:
    while True:
        # Load the current watchlist from local storage, which is a JSON file
        tickers = watchlist.load_watchlist()

        if tickers:
            logger.info(f"Refreshing {len(tickers)} watchlist ticker(s): {tickers}")
            # For each ticker in the watchlist, fetch its historical data for the past 5 years and upload it to S3
            for ticker in tickers:
                try:
                    df = fetch_all([ticker], years=5)
                    s3_data.upload_ticker_data(df, bucket_name, ticker)
                except Exception as exc:
                    logger.error(f"Failed to refresh {ticker}: {exc}")
        else:
            logger.info("Watchlist is empty, nothing to refresh.")
        time.sleep(interval_seconds)