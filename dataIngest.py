"""
This is a simple python script to pull historical stock price data
from Yahoo Finance and save it to local Parquet files. It uses the
yfinance library to fetch the data and pandas to process and save it.

Goal is to pull daily OHLCV (Open/High/Low/Close/Volume) data from Yahoo Finance
for a configurable set of tickers, cleans it up, and writes it to
local Parquet files (one per ticker, plus a combined file). This is
meant to be the local/offline first step before wiring the same logic
into an S3 upload + AWS Lambda job later.

Usage:
    python dataIngest.py
    python dataIngest.py --tickers AAPL MSFT GOOGL --years 5
    python dataIngest.py --bucket ziwen-wang-stock-data-project
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
import yfinance as yf
from botocore.exceptions import ClientError, NoCredentialsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# A default basket of tickers to fetch if none are provided on the command line.
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",   # Tech
    "JPM", "GS", "BAC",                          # Financials
    "XOM", "CVX",                                 # Energy
    "JNJ", "PFE",                                 # Healthcare
    "SPY",                                        # Broad market benchmark
]

# Where to save the Parquet files locally.
OUTPUT_DIR = Path("data/raw")

"""
This function fetches historical daily OHLCV data for a single ticker
from the Yahoo Finance API using the yfinance library.
It takes the ticker symbol, start date, and end date as input parameters
and returns a pandas DataFrame containing the historical data.
"""
def fetch_ticker_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    # Logs the ticker and date range being fetched
    logger.info(f"Fetching {ticker} from {start} to {end}")

    # Fetch the data using yfinance. The history() method returns a DataFrame
    # with the historical data for the specified ticker and date range.
    # The interval is set to "1d" for daily data.
    df = yf.Ticker(ticker).history(start=start, end=end, interval="1d")

    # If the DataFrame is empty, log a warning and return the empty DataFrame.
    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return df

    # Reset the index of the DataFrame, which is typically the date, and rename
    # the columns to lowercase with underscores instead of spaces.
    # Also adds the ticker symbol column and the ingestion timestamp
    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df["ticker"] = ticker
    df["ingested_at"] = datetime.now().isoformat()

    # Keep only the relevant columns for analysis and storage.
    keep_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    df = df[[c for c in keep_cols if c in df.columns] + ["ingested_at"]]

    return df

"""
This function fetches historical data for a list of tickers over a specified
number of years. It iterates over each ticker, fetching its historical data using the fetch_ticker_history function. 
Fetched data is concatenated into a single DataFrame, which is then sorted and returned.
"""
def fetch_all(tickers: list[str], years: int) -> pd.DataFrame:
    # Calculates the start and end dates for the fetch
    end = datetime.now().date()
    start = end - timedelta(days=365 * years)

    # Holds the DataFrames for each successfully fetched ticker
    frames = []
    # Holds the tickers that failed to fetch data
    failed = []

    # Iterates over each ticker, fetching its historical data
    for ticker in tickers:
        try:
            df = fetch_ticker_history(ticker, str(start), str(end))
            if not df.empty:
                frames.append(df)
            else:
                failed.append(ticker)
        except Exception as exc:
            logger.error(f"Failed to fetch {ticker}: {exc}")
            failed.append(ticker)

    # If any tickers failed to fetch data, log a warning with their names
    if failed:
        logger.warning(f"Tickers with no data / errors: {failed}")

    # If no data was fetched for any ticker, raise a RuntimeError
    if not frames:
        raise RuntimeError("No data fetched for any ticker — aborting.")

    # Concatenate all the DataFrames into a single DataFrame, ignoring the index
    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    return combined.sort_values(["ticker", "date"]).reset_index(drop=True)

"""
This function facilitates the saving of the fetched data to local Parquet files.
"""
def save_outputs(df: pd.DataFrame, output_dir: Path) -> None:
    # Creates the output directory if it doesn't exist, including any necessary parent directories. 
    output_dir.mkdir(parents=True, exist_ok=True)

    # Saves the combined DataFrame to a single Parquet file named "combined_prices.parquet" in the specified output directory.
    combined_path = output_dir / "combined_prices.parquet"
    df.to_parquet(combined_path, index=False)
    logger.info(f"Wrote combined file: {combined_path} ({len(df):,} rows)")

    # Saves each ticker's data to separate Parquet files named after
    # each ticker symbol in the specified output directory.
    for ticker, group in df.groupby("ticker"):
        ticker_path = output_dir / f"{ticker}.parquet"
        group.to_parquet(ticker_path, index=False)
    logger.info(f"Wrote {df['ticker'].nunique()} per-ticker Parquet files to {output_dir}")

"""
This function facilitates the uploading of local Parquet files to an S3 bucket.
"""
def upload_to_s3(local_dir: Path, bucket: str, prefix: str = "raw") -> None:
    # boto3.client("s3") creates a connection to S3 using credentials
    # saved to my local machine via `aws configure`.
    s3 = boto3.client("s3")

    # Creates a list of files to upload by searching for all Parquet files
    files_to_upload = list(local_dir.glob("*.parquet"))
    if not files_to_upload:
        logger.warning(f"No Parquet files found in {local_dir} to upload.")
        return
    logger.info(f"Uploading {len(files_to_upload)} file(s) to s3://{bucket}/{prefix}/")

    # Iterates over each file in the list of files to upload
    for file_path in files_to_upload:
        # Constructs the S3 path for the file by combining the prefix and the file name
        s3_key = f"{prefix}/{file_path.name}"
        try:
            s3.upload_file(str(file_path), bucket, s3_key)
            logger.info(f"  -> uploaded {file_path.name} to s3://{bucket}/{s3_key}")
        except NoCredentialsError:
            # This means `aws configure` either wasn't run, or the
            # credentials file can't be found.
            logger.error(
                "No AWS credentials found. Run 'aws configure' in your "
                "terminal and make sure your Access Key ID / Secret Key "
                "are entered correctly."
            )
            return
        except ClientError as exc:
            # This usually means the bucket name is wrong, doesn't exist,
            # or the IAM user doesn't have permission to write to it.
            logger.error(f"  -> failed to upload {file_path.name}: {exc}")
    logger.info("S3 upload step complete.")

"""
This function parses command-line arguments for the script,
allowing for users to customize the tickers, years of history,
output directory, S3 bucket, and S3 prefix.
"""
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest historical OHLCV data from Yahoo Finance.")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=DEFAULT_TICKERS,
        help="List of ticker symbols to fetch (default: a preset diversified basket).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Number of years of history to fetch (default: 5).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory to write Parquet files to (default: data/raw).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help=(
            "S3 bucket name to upload the Parquet files to. If omitted, "
            "the script only saves locally and skips the S3 upload step."
        ),
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="raw",
        help="S3 key prefix (like a folder path) to upload files under (default: raw).",
    )
    return parser.parse_args()


def main() -> None:
    # Parse command-line arguments
    args = parse_args()
    # Fetch historical stock data for the specified tickers and years
    df = fetch_all(args.tickers, args.years)
    # Save the fetched data to local Parquet files in the specified output directory
    save_outputs(df, args.output_dir)

    logger.info(
        f"Done. {len(df):,} rows across {df['ticker'].nunique()} tickers, "
        f"date range {df['date'].min()} to {df['date'].max()}."
    )

    # Uploading to S3 is optional: it only happens if the --bucket argument is provided.
    # This keeps local testing simple (no AWS needed) while still
    # supporting the upload step for later use in a Lambda job or other cloud-based workflow.
    if args.bucket:
        upload_to_s3(args.output_dir, args.bucket, args.s3_prefix)
    else:
        logger.info("No --bucket provided, skipping S3 upload.")


if __name__ == "__main__":
    main()