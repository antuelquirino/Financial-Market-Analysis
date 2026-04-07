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
TABLE = "market_prices"

START_DATE = "2021-01-01"
END_DATE = None

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
# 4. Market data loader
# =====================================================
def load_market_data(ticker, start, end=None):
    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False
    )

    df.reset_index(inplace=True)

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df["ticker"] = ticker

    return df

# =====================================================
# 5. Extract
# =====================================================
dfs = []

for ticker in TICKERS:
    print(f"Downloading {ticker}...")
    dfs.append(load_market_data(ticker, START_DATE, END_DATE))

df_market = pd.concat(dfs, ignore_index=True)

# Keep only required columns
df_market = df_market[[
    "date", "ticker", "open", "high", "low", "close", "volume"
]]

df_market.drop_duplicates(subset=["ticker", "date"], inplace=True)

# =====================================================
# 6. Load to BigQuery
# =====================================================
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    df_market,
    TABLE_ID,
    job_config=job_config
)

job.result()

print(f"Loaded {len(df_market)} rows into {TABLE_ID}")




