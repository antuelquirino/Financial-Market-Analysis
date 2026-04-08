import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

# Config
PROJECT_ID = "financial-market-analysis"
DATASET = "raw_finance"
TABLE = "market_prices"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
TICKERS = ["AAPL", "AMZN", "NVDA", "XOM", "CVX", "JPM", "C", "^GSPC"]

def get_client():
    # 1. Intentamos leer la variable de entorno que definimos en el YAML
    service_account_info_str = os.environ.get('GCP_SA_KEY_JSON')
    
    if service_account_info_str:
        print("🔐 Credenciales detectadas en variable de entorno.")
        info = json.loads(service_account_info_str)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        # 2. Si no existe (estás local), busca el archivo físico o ADC
        print("💻 Usando autenticación local/ADC.")
        return bigquery.Client(project=PROJECT_ID)

def run_incremental_extraction():
    client = get_client() # USAMOS LA FUNCIÓN QUE CREAMOS ARRIBA
    
    print(f"📥 Descargando precios recientes para: {TICKERS}")
    df = yf.download(TICKERS, period="5d", interval="1d", auto_adjust=True)
    
    # Reset de índice para limpiar el formato de yfinance
    df = df['Close'].stack().reset_index()
    df.columns = ['date', 'ticker', 'close']
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    print(f"📤 Subiendo datos a {TABLE_ID}...")
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"✅ ¡Éxito! Se agregaron {len(df)} filas.")

if __name__ == "__main__":
    run_incremental_extraction()