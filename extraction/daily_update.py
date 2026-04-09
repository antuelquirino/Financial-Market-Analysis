import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

PROJECT_ID = "financial-market-analysis"
DATASET = "raw_finance"
TABLE = "market_prices"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
TICKERS = ["AAPL", "AMZN", "NVDA", "XOM", "CVX", "JPM", "C", "^GSPC"]

def get_client():
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    key_json = os.environ.get('GCP_SA_KEY_JSON')
    
    if key_json:
        info = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        return bigquery.Client(project=PROJECT_ID)

def run_incremental_extraction():
    client = get_client()
    
    df = yf.download(TICKERS, period="5d", interval="1d")
    df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
    df = df.stack(level=1).reset_index()
    df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    df.drop_duplicates(subset=["ticker", "date"], inplace=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()

if __name__ == "__main__":
    run_incremental_extraction()