"""
Flask backend for the stock dashboard.

There exists two separate data paths in this app:
    WATCHLIST tickers:  fetched regularly by a background thread ->
                        stored in S3 (overwritten each refresh) ->
                        queried with DuckDB reading straight from S3.

    AD-HOC tickers      -> fetched once, on request -> held in a plain
                        Python dict in memory -> queried with DuckDB
                        reading that in-memory DataFrame directly ->
                        vanishes when this process exits (app closed).

Both paths share the exact same fetching logic from dataIngest.py

Run with:
    python app.py
"""
import logging
import os
import threading

import duckdb
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS

import s3_data
import watchlist
from dataIngest import fetch_all
from refresher import refresh_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
# CORS(app) allows the frontend (running on a different port)
# to make requests to this backend without being blocked by
# the browser's same-origin policy.
CORS(app)

# The S3 bucket watchlist data gets written to / read from / deleted from.
# Can be overridden with an environment variable.
BUCKET_NAME = os.environ.get("BUCKET_NAME", "ziwen-wang-stock-data-project")

# How often the background thread re-fetches watchlist tickers, in seconds.
REFRESH_INTERVAL_SECONDS = 1 * 60  # 1 minute

# Ad-hoc lookups live ONLY here, in memory, for the lifetime of this
# process. Nothing ever writes this dict to disk or S3.
# Shape: {"AAPL": <DataFrame>, "TSLA": <DataFrame>, ...}
ad_hoc_cache: dict[str, pd.DataFrame] = {}

# A lock to prevent the background refresh thread and an incoming HTTP
# request from both reading/writing shared state at the exact same
# moment. Flask's dev server can handle requests on different threads,
# so without this, rare but real race conditions become possible.
ad_hoc_lock = threading.Lock()

# ---------------------------------------------------------------------
# DuckDB querying — the same query-building logic serves both the
# S3-backed watchlist path and the in-memory ad-hoc path, since DuckDB
# can point its FROM clause at either kind of source.
# ---------------------------------------------------------------------
"""
Create a fresh DuckDB connection with S3 access configured.
DuckDB's "httpfs" extension is what lets it read
files sitting in S3 as if they were local files.
"""
def _get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    return con

"""
Build the analysis SQL query. `from_clause` is either an S3 Parquet
reference (for watchlist data) or a plain table/variable name (for
an in-memory ad-hoc DataFrame) — the rest of the query is identical
either way, which is the whole point of using DuckDB for both paths.
"""
def _build_query(from_clause: str, metric: str, window: int = 20) -> str:
    if metric == "moving_average":
        return f"""
            SELECT
                ticker, date, close,
                AVG(close) OVER (
                    PARTITION BY ticker ORDER BY date
                    ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
                ) AS moving_avg
            FROM {from_clause}
            ORDER BY ticker, date
        """
    elif metric == "returns":
        return f"""
            SELECT
                ticker, date, close,
                (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date))
                    / LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS daily_return
            FROM {from_clause}
            ORDER BY ticker, date
        """
    else:  # "raw" or anything unrecognized falls back to the unmodified rows
        return f"SELECT * FROM {from_clause} ORDER BY ticker, date"

# ---------------------------------------------------------------------
# Watchlist management endpoints
# ---------------------------------------------------------------------
@app.route("/api/watchlist", methods=["GET"])
def get_watchlist():
    return jsonify({"tickers": watchlist.load_watchlist()})

@app.route("/api/watchlist", methods=["POST"])
def add_to_watchlist():
    """
    Add one or more tickers at once. Expects a JSON body like:
        {"tickers": ["AAPL", "MSFT", "TSLA"]}

    A single-ticker add is just a list with one item — the frontend
    always sends a list, so there's only one code path to maintain
    here instead of separate single/bulk handling.
    """
    body = request.get_json(force=True)
    raw_tickers = (body or {}).get("tickers", [])

    if not isinstance(raw_tickers, list) or not raw_tickers:
        return jsonify({"error": "tickers must be a non-empty list"}), 400

    normalized = [t.strip().upper() for t in raw_tickers if t.strip()]
    updated_list, added = watchlist.add_tickers(normalized)

    # Only fetch/upload for tickers that were ACTUALLY newly added
    results = {ticker: "already_tracked" for ticker in normalized if ticker not in added}

    if added:
        # Fetch data for all newly-added tickers with ONE fetch_all()
        # call, rather than looping and calling it once per ticker.
        try:
            combined_df = fetch_all(added, years=5)
            for ticker, group in combined_df.groupby("ticker"):
                try:
                    s3_data.upload_ticker_data(group, BUCKET_NAME, ticker)
                    results[ticker] = "ok"
                except Exception as exc:
                    logger.error(f"Upload failed for {ticker}: {exc}")
                    results[ticker] = f"upload_failed: {exc}"
        except Exception as exc:
            logger.error(f"Bulk fetch failed for {added}: {exc}")
            for ticker in added:
                results[ticker] = f"fetch_failed: {exc}"
    return jsonify({"tickers": updated_list, "fetch_results": results})


@app.route("/api/watchlist", methods=["DELETE"])
def remove_from_watchlist():
    """
    Remove one or more tickers at once. Expects a JSON body like:
        {"tickers": ["AAPL", "MSFT"]}
    """
    body = request.get_json(force=True)
    raw_tickers = (body or {}).get("tickers", [])

    if not isinstance(raw_tickers, list) or not raw_tickers:
        return jsonify({"error": "tickers must be a non-empty list"}), 400

    normalized = [t.strip().upper() for t in raw_tickers if t.strip()]
    updated_list, removed = watchlist.remove_tickers(normalized)

    # Only delete from S3 for tickers that were ACTUALLY in the
    # watchlist and got removed — not every ticker that was requested.
    s3_results = {}
    for ticker in removed:
        s3_results[ticker] = s3_data.delete_ticker_data(BUCKET_NAME, ticker)
    return jsonify({"tickers": updated_list, "s3_deleted": s3_results})


# ---------------------------------------------------------------------
# Data + analysis endpoints
# ---------------------------------------------------------------------
@app.route("/api/watchlist/data", methods=["GET"])
def get_watchlist_data():
    """Query watchlist tickers' data, straight from S3, via DuckDB."""
    metric = request.args.get("metric", "raw")
    window = int(request.args.get("window", 20))
    ticker = request.args.get("ticker")  # optional: filter to one ticker

    if ticker:
        from_clause = f"read_parquet('s3://{BUCKET_NAME}/watchlist/{ticker.upper()}.parquet')"
    else:
        from_clause = f"read_parquet('s3://{BUCKET_NAME}/watchlist/*.parquet')"

    query = _build_query(from_clause, metric, window)

    try:
        con = _get_duckdb_connection()
        result = con.execute(query).fetchdf()
        return result.to_json(orient="records", date_format="iso")
    except Exception as exc:
        logger.error(f"Watchlist query failed: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/ticker/<symbol>", methods=["GET"])
def get_ad_hoc_ticker(symbol):
    """
    Look up any ticker, watchlist or not, without ever writing it to S3.
    If we've already fetched it this session, reuse the cached copy
    instead of hitting Yahoo Finance again.
    """
    symbol = symbol.strip().upper()
    metric = request.args.get("metric", "raw")
    window = int(request.args.get("window", 20))

    with ad_hoc_lock:
        if symbol not in ad_hoc_cache:
            try:
                ad_hoc_cache[symbol] = fetch_all([symbol], years=5)
            except Exception as exc:
                logger.error(f"Ad-hoc fetch for {symbol} failed: {exc}")
                return jsonify({"error": str(exc)}), 500

        # Give DuckDB a local variable to point its FROM clause at.
        # DuckDB can query a pandas DataFrame directly by the name of
        # the Python variable holding it — no need to save it anywhere first.
        df = ad_hoc_cache[symbol]

    query = _build_query("df", metric, window)

    try:
        con = _get_duckdb_connection()
        result = con.execute(query).fetchdf()
        return result.to_json(orient="records", date_format="iso")
    except Exception as exc:
        logger.error(f"Ad-hoc query failed for {symbol}: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------
# App startup
# ---------------------------------------------------------------------
def main():
    # daemon=True means this thread won't prevent the app from exiting
    # when you close it — a non-daemon thread would keep the process
    # alive in the background even after the main app "closes", which
    # would silently defeat the "only refreshes while open" design.
    refresh_thread = threading.Thread(
        target=refresh_loop,
        args=(BUCKET_NAME, REFRESH_INTERVAL_SECONDS),
        daemon=True,
    )
    refresh_thread.start()

    app.run(debug=True, port=5000, use_reloader=False)
    # use_reloader=False matters here: Flask's debug reloader normally
    # starts a SECOND copy of your whole app (including our background
    # thread) to watch for code changes, which would mean two refresh
    # loops running at once, silently doubling your Yahoo Finance calls.


if __name__ == "__main__":
    main()