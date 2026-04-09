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

def load_ticker(ticker):
    # Descargamos 5 años de datos
    df = yf.download(ticker, period="5y", interval="1d")
    
    # FIX: Si yfinance devuelve MultiIndex (ej. Price y Ticker), lo aplanamos
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    df.reset_index(inplace=True)

    # Pasamos columnas a minúsculas (ahora que el índice es simple, .str funcionará)
    df.columns = [col.lower() for col in df.columns]

    # Mapeo de seguridad por si las columnas vienen como 'adj close'
    df = df.rename(columns={
        "adj close": "close"
    })

    df["ticker"] = ticker

    # Retornamos el set completo de columnas para que dbt no falle
    return df[["date", "ticker", "open", "high", "low", "close", "volume"]]

def run_incremental_extraction():
    client = get_client()
    
    dfs = []
    for ticker in TICKERS:
        print(f"Descargando {ticker}...")
        try:
            data = load_ticker(ticker)
            if not data.empty:
                dfs.append(data)
        except Exception as e:
            print(f"Error con {ticker}: {e}")

    if not dfs:
        return

    df = pd.concat(dfs, ignore_index=True)

    # Limpieza de fechas para BigQuery
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    df.drop_duplicates(subset=["ticker", "date"], inplace=True)

    # TRUNCATE es clave: borra lo viejo y sube los 5 años limpios
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    print(f"Subiendo datos a BigQuery ({TABLE_ID})...")
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print("¡Proceso completado con éxito!")

if __name__ == "__main__":
    run_incremental_extraction()