"""
Handles putting watchlist tickers' data into S3, and removing it when a
ticker gets dropped from the watchlist.

All watchlist data lives under the "watchlist/" prefix in the bucket,
one Parquet file per ticker (e.g. "watchlist/AAPL.parquet"), to keep it
clearly separate from any other data users might store in the same
bucket later.
"""
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

WATCHLIST_PREFIX = "watchlist"

"""
Build the S3 key for a given ticker's data file.
"""
def _ticker_key(ticker: str) -> str:
    return f"{WATCHLIST_PREFIX}/{ticker.upper()}.parquet"

"""
Write a ticker's DataFrame to S3, overwriting whatever was there
before. This is what the background refresh thread calls on every
tick, for every watchlist ticker.
"""
def upload_ticker_data(df: pd.DataFrame, bucket: str, ticker: str) -> None:
    import io

    buffer = io.BytesIO()
    # Writes the DataFrame to the in-memory buffer in Parquet format, without including the index.
    df.to_parquet(buffer, index=False)
    # Resets the buffer's position to the beginning so that the entire content can be read when uploading to S3.
    buffer.seek(0)

    # Constructs the S3 key for the ticker's data file using the _ticker_key helper function.
    key = _ticker_key(ticker)
    # Creates an S3 client using boto3, which will use the AWS credentials configured on the local machine.
    s3 = boto3.client("s3")
    # Uploads the in-memory Parquet data to the specified S3 bucket and key, effectively overwriting any existing file for that ticker.
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Uploaded {ticker} ({len(df):,} rows) to s3://{bucket}/{key}")

"""
Delete a ticker's data file from S3. Called when a ticker is
removed from the watchlist, so its data doesn't linger in the
bucket (and keep costing you storage) after you've stopped tracking it.

Returns True if the delete call succeeded, False otherwise — the
caller decides how to react.
"""
def delete_ticker_data(bucket: str, ticker: str) -> bool:
    # Constructs the S3 key for the ticker's data file using the _ticker_key helper function.
    key = _ticker_key(ticker)
    # Creates an S3 client using boto3, which will use the AWS credentials configured on the local machine.
    s3 = boto3.client("s3")

    # Attempts to delete the specified object from the S3 bucket.
    # If successful, logs the deletion and returns True. 
    # If a ClientError occurs, logs the error and returns False.
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted s3://{bucket}/{key}")
        return True
    except ClientError as exc:
        logger.error(f"Failed to delete s3://{bucket}/{key}: {exc}")
        return Falsewa