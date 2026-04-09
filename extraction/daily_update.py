import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

# =====================================================
# 1. Configuración
# =====================================================
PROJECT_ID = "financial-market-analysis"
DATASET = "raw_finance"
TABLE = "market_prices"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

START_DATE = "2021-01-01" # 5 años aprox de historia

TICKERS = ["AAPL", "AMZN", "NVDA", "XOM", "CVX", "JPM", "C", "^GSPC"]

# =====================================================
# 2. Cliente de BigQuery (Compatible con GitHub)
# =====================================================
def get_client():
    key_json = os.environ.get('GCP_SA_KEY_JSON')
    if key_json:
        info = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return bigquery.Client(project=PROJECT_ID)

# =====================================================
# 3. Cargador de datos (Con arreglo MultiIndex)
# =====================================================
def load_market_data(ticker, start):
    # Descarga
    df = yf.download(ticker, start=start, auto_adjust=True, progress=False)
    
    # IMPORTANTE: Resetear índice primero
    df.reset_index(inplace=True)

    # REGLA DE ORO: Si es MultiIndex, aplanar ANTES de cualquier otra cosa
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Ahora sí podemos usar .str.lower()
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    
    df["ticker"] = ticker
    return df

# =====================================================
# 4. Ejecución Principal
# =====================================================
def run_pipeline():
    client = get_client()
    dfs = []

    for ticker in TICKERS:
        print(f"Downloading {ticker}...")
        try:
            temp_df = load_market_data(ticker, START_DATE)
            if not temp_df.empty:
                dfs.append(temp_df)
        except Exception as e:
            print(f"Error con {ticker}: {e}")

    if not dfs:
        print("No se descargaron datos.")
        return

    df_market = pd.concat(dfs, ignore_index=True)

    # Seleccionar solo lo que dbt espera
    df_market = df_market[["date", "ticker", "open", "high", "low", "close", "volume"]]

    # Limpiar fechas para BigQuery
    df_market['date'] = pd.to_datetime(df_market['date']).dt.tz_localize(None)
    df_market.drop_duplicates(subset=["ticker", "date"], inplace=True)

    # Carga con TRUNCATE (Borra y reemplaza con historia limpia)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    print(f"Cargando {len(df_market)} filas en BigQuery...")
    job = client.load_table_from_dataframe(df_market, TABLE_ID, job_config=job_config)
    job.result()
    print("¡Éxito!")

if __name__ == "__main__":
    run_pipeline()