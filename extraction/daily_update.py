import pandas as pd
import yfinance as yf
from google.cloud import bigquery

# Config
PROJECT_ID = "financial-market-analysis"
DATASET = "raw_finance"
TABLE = "market_prices"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

TICKERS = ["AAPL", "AMZN", "NVDA", "XOM", "CVX", "JPM", "C", "^GSPC"]

client = bigquery.Client(project=PROJECT_ID)

def run_incremental_extraction():
    # Bajamos los últimos 5 días para cubrir fines de semana o feriados
    print(f"📥 Descargando precios recientes para: {TICKERS}")
    df = yf.download(TICKERS, period="5d", interval="1d", auto_adjust=True)
    
    # Formatear el DataFrame de Yahoo (Close prices)
    df = df['Close'].stack().reset_index()
    df.columns = ['date', 'ticker', 'close']
    
    # Limpiar la fecha (quitar zona horaria para BigQuery)
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)

    # CONFIGURACIÓN DE CARGA: 
    # Usamos WRITE_APPEND para que se sumen a la tabla existente
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    print(f"📤 Subiendo datos a BigQuery...")
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"✅ ¡Éxito! Se agregaron {len(df)} filas.")

if __name__ == "__main__":
    run_incremental_extraction()