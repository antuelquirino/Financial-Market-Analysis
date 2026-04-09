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
START_DATE = "2021-01-01" 
TICKERS = ["AAPL", "AMZN", "NVDA", "XOM", "CVX", "JPM", "C", "^GSPC"]

def get_client():
    key_json = os.environ.get('GCP_SA_KEY_JSON')
    if key_json:
        info = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return bigquery.Client(project=PROJECT_ID)

def load_market_data(ticker, start):
    df = yf.download(ticker, start=start, auto_adjust=True, progress=False)
    df.reset_index(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df["ticker"] = ticker
    return df

def run_pipeline():
    client = get_client()
    dfs = []
    for ticker in TICKERS:
        try:
            temp_df = load_market_data(ticker, START_DATE)
            if not temp_df.empty:
                dfs.append(temp_df)
        except Exception as e:
            print(f"Error en {ticker}: {e}")
    if not dfs: return
    df_market = pd.concat(dfs, ignore_index=True)
    df_market['date'] = pd.to_datetime(df_market['date']).dt.tz_localize(None)
    df_market.drop_duplicates(subset=["ticker", "date"], inplace=True)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df_market, TABLE_ID, job_config=job_config)
    job.result()
    print("Carga terminada")

if __name__ == "__main__":
    run_pipeline()