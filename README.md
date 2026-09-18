# StockTracker
This data pipeline project is meant to allow for users to find historical daily OHLCV data for stocks of their choosing and perform analysis on these stocks to gain insights.

Current Capabilities:
A simple python script to pull historical stock price data from Yahoo Finance and save it to local Parquet files. It uses the yfinance library to fetch the data and pandas to process and save it.

Currently pulls daily OHLCV (Open/High/Low/Close/Volume) data from Yahoo Finance
for a configurable set of tickers, cleans it up, and writes it to
local Parquet files (one per ticker, plus a combined file). Optionally, users can specify a S3 bucket to upload the Parquet files into AWS.

Usage:
    python dataIngest.py

Optional Command-Line Arguments:
    --tickers
        Provide a list of tickers to fetch data for
        (default: AAPL, MSFT, GOOGL, AMZN, NVDA, JPM, GS, BAC, XOM, CVX, JNJ, PFE, SPY)
    --years
        Provide an integer (minimum 1) amount of years of data to fetch
        (default: 5)
    --output-dir
        Provide an output directory path to store local Parquet files
        (default: data/raw)
    --bucket
        Provide a S3 bucket name to allow for upload to AWS.
        (default: skips the S3 upload and only saves locally)
    --s3-prefix
        Provide a S3 key prefix (like a folder path) to upload files under
        (default: raw)
    
Work-in-progress Components:
- Automate the fetch and upload process using AWS Lambda to allow for periodic updating of data without user intervention
- Use Pandas and SQL to perform analysis on the fetched data and generate insights
- Create an user dashboard to allow for aesthetic display of information