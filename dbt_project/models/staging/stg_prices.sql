{{ config(
    materialized='view'
) }}

-- Staging table for market prices
-- Solo pedimos lo que realmente se cargó en BigQuery para evitar errores

SELECT
    `date`,
    ticker,
    close
FROM 
    {{ source('raw_finance', 'market_prices') }}
