# %%
# =====================================================
# 1. Libraries
# =====================================================
import pandas as pd
import yfinance as yf
from google.cloud import bigquery

# =====================================================
# 2. Config
# =====================================================
PROJECT_ID = "financial-market-analysis"
DATASET = "raw_finance"
TABLE = "company_metadata"

TICKERS = {
    "AAPL": "Apple",
    "AMZN": "Amazon",
    "NVDA": "Nvidia",
    "XOM": "ExxonMobil",
    "CVX": "Chevron",
    "JPM": "JPMorgan",
    "C": "Citi",
    "^GSPC": "S&P 500"
}

# =====================================================
# 3. BigQuery client
# =====================================================
client = bigquery.Client(project=PROJECT_ID)
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# =====================================================
# 4. Metadata extractor
# =====================================================
metadata_rows = []

for ticker, fallback_name in TICKERS.items():
    print(f"Fetching metadata for {ticker}...")
    try:
        info = yf.Ticker(ticker).info

        metadata_rows.append({
            "ticker": ticker,
            "company_name": info.get("longName", fallback_name),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": info.get("marketCap"),
            "country": info.get("country"),
            "exchange": info.get("exchange")
        })

    except Exception as e:
        print(f"⚠️ Error fetching {ticker}: {e}")

# =====================================================
# 5. Create DataFrame
# =====================================================
df_metadata = pd.DataFrame(metadata_rows)

# Enforce uniqueness
df_metadata.drop_duplicates(subset=["ticker"], inplace=True)

# =====================================================
# 6. Load to BigQuery
# =====================================================
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    df_metadata,
    TABLE_ID,
    job_config=job_config
)

job.result()

print(f"Loaded {len(df_metadata)} rows into {TABLE_ID}")


# %%



