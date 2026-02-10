from google.cloud import bigquery
from bigquery_client import get_client

def get_tickers():
    client = get_client()

    query = """
        SELECT DISTINCT ticker
        FROM raw_finance.mart_prices
        ORDER BY ticker
    """

    return [row["ticker"] for row in client.query(query).result()]


def get_ticker_data(ticker, start_date=None, end_date=None):
    client = get_client()

    query = """
        SELECT
            date,
            ticker,
            price,
            daily_return,
            cum_return,
            volatility,
            drawdown,
            cagr,
            sharpe_ratio
        FROM raw_finance.mart_prices
        WHERE ticker = @ticker
    """

    params = [
        bigquery.ScalarQueryParameter("ticker", "STRING", ticker)
    ]

    if start_date:
        query += " AND date >= @start_date"
        params.append(
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date)
        )

    if end_date:
        query += " AND date <= @end_date"
        params.append(
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
        )

    query += " ORDER BY date"

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    return client.query(query, job_config=job_config).to_dataframe()


