#  Financial Market Analysis Platform

A production-style financial analytics platform built using modern data engineering and analytics engineering best practices.

This project extracts market data, transforms it using dbt, stores it in BigQuery, and delivers interactive analytics through Streamlit and Tableau.

---

##  Live Applications

###  Streamlit App (Interactive Dashboard)
https://financial-market-analysis-eebjsbfnfsd57txv6wrgra.streamlit.app/

Interactive performance dashboard with:
- Sharpe Ratio
- Volatility
- Max Drawdown
- CAGR / Period Return
- Rolling risk analytics

---

###  Tableau Dashboard (Sector Comparison)
https://public.tableau.com/app/profile/antuel.quirino/viz/Perfomancevs_Benchmark/Dashboard1?publish=yes

Sector-based performance comparison and cumulative return analysis.

---

###  GitHub Repository
https://github.com/antuelquirino/Financial-Market-Analysis

---

#  Architecture Overview

### This project follows a layered data architecture:

Yahoo Finance API

Extraction Notebooks (Python + yfinance)

Google BigQuery (raw layer)

dbt (staging → intermediate → mart)

Streamlit (Python dashboard)

Tableau (BI visualization layer)

---

##  dbt Layered Modeling

###  Staging Layer
- `stg_prices`
- `stg_metadata`

Standardized raw financial data.

###  Intermediate Layer
- `i_returns`
- `i_cumulative`
- `i_volatility`
- `i_drawdown`
- `i_sharpe`
- `i_cagr`

Financial risk & return logic:
- Log-compounded cumulative returns
- Rolling annualized volatility
- Period-based Sharpe Ratio
- Peak-to-trough drawdown calculation

###  Mart Layer
- `mart_prices`

Final analytics table consolidating:
- Price
- Returns
- Risk metrics
- Company metadata

This is the primary table consumed by dashboards.

---


#  Financial Metrics Implemented

### Sharpe Ratio
Period-based, annualized:
(mean excess return / std deviation) × √252

### Volatility
Realized volatility of selected period:
std(daily returns) × √252

### Max Drawdown
Peak-to-trough decline within selected time window.

### CAGR
Compound annual growth rate:
(final / initial)^(1/years) - 1

### Cumulative Return
Log-compounded:
exp(sum(ln(1 + daily_return))) - 1

---

#  Streamlit Dashboard Features

- Dynamic time range selection (1M, 6M, 1Y, 3Y, 5Y)
- Period-aware KPIs
- Rolling volatility chart
- Benchmark comparison (^GSPC)
- Interactive visualizations via Altair
- Secure BigQuery connection via Streamlit Secrets

---

#  Tableau Dashboard Features

- Sector comparison
- Benchmark analysis
- Cumulative return scaling
- Ticker highlighting
- Interactive filtering

---

#  Tech Stack

| Layer | Technology |
|--------|------------|
| Data Extraction | Python, yfinance |
| Data Warehouse | Google BigQuery |
| Transformations | dbt (local) |
| Analytics | SQL, Financial Modeling |
| Dashboard | Streamlit |
| BI | Tableau |
| Version Control | GitHub |

---

#  Security & Deployment

- Service account credentials managed via Streamlit Secrets
- No credentials stored in repository
- Streamlit deployed via Streamlit Cloud
- BigQuery as centralized data warehouse

---

#  Screenshots

## Streamlit Dashboard
![Streamlit Dashboard](screenshots/Streamlit_image.png)

## Tableau Dashboard
![Tableau Dashboard](screenshots/Perfomance_vs_benchmark.png)

## dbt Lineage Graph
![dbt Lineage](screenshots/lineage graph.png)

---

#  Local Setup (For Developers)

This project does NOT include service account credentials.

To run locally:

1. Create your own Google Cloud project
2. Enable BigQuery API
3. Create a service account
4. Add credentials to `.streamlit/secrets.toml`

Example structure:

[gcp_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth
"
token_uri = "https://oauth2.googleapis.com/token
"

Then:

pip install -r requirements.txt
streamlit run app/main.py

---



# Technical Roadmap

Incremental Loading: Transition dbt models to incremental materialization for performance.

Orchestration: Implement GitHub Actions to automate the daily extraction and dbt run.

Unit Testing: Add dbt data tests for financial threshold validation.

# Project Objective
This project simulates a production-grade environment, integrating Data Engineering, Analytics Engineering, and Financial Modeling. It is designed to demonstrate proficiency in cloud-based data stacks (GCP) and the separation of concerns between data transformation and visualization.

# Author: Antuel Quirino

