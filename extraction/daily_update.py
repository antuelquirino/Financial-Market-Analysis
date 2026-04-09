import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

print("🚀 RUNNING DAILY_UPDATE (DIRECT INJECTION MODE)")

# Config
PROJECT_ID = "financial-market-analysis"
DATASET = "raw_finance"
TABLE = "market_prices"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
TICKERS = ["AAPL", "AMZN", "NVDA", "XOM", "CVX", "JPM", "C", "^GSPC"]

def get_client():
    # Remove environment variables that might force the library to look for a physical file
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    key_json = os.environ.get('GCP_SA_KEY_JSON')
    
    if key_json:
        print("🔐 Authenticating using secret from memory...")
        info = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        print("💻 Using local environment authentication (ADC)...")
        return bigquery.Client(project=PROJECT_ID)

def run_incremental_extraction():
    client = get_client()
    
    print(f"📥 Downloading data for: {TICKERS}")
    df = yf.download(TICKERS, period="5d", interval="1d", auto_adjust=True)
    
    # Handle multi-index if necessary and format dataframe
    df = df['Close'].stack().reset_index()
    df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
    
    # Remove timezone for BigQuery compatibility
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    print(f"📤 Uploading data to {TABLE_ID}...")
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    print(f"✅ Success! Added {len(df)} rows.")

if __name__ == "__main__":
    run_incremental_extraction()