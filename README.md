# Financial Market Analysis Platform

A production-style financial analytics platform built using modern data engineering and analytics engineering best practices.

This project extracts market data, transforms it using dbt, stores it in BigQuery, and delivers interactive analytics through Streamlit and Tableau.

---

## Live Applications

## Streamlit App (Interactive Dashboard)
https://financial-market-analysis-eebjsbfnfsd57txv6wrgra.streamlit.app/

Interactive performance dashboard with:
- Sharpe Ratio
- Volatility
- Max Drawdown
- CAGR / Period Return
- Rolling risk analytics

---

## Tableau Dashboard (Sector Comparison)
https://public.tableau.com/app/profile/antuel.quirino/viz/Perfomancevs_Benchmark/Dashboard1?publish=yes

Sector-based performance comparison and cumulative return analysis.

---

## GitHub Repository
https://github.com/antuelquirino/Financial-Market-Analysis

---

# Architecture and Automation

## This project follows a layered data architecture:

1. Yahoo Finance API
2. Extraction (Python + yfinance)
3. Google BigQuery (Raw layer)
4. dbt (Staging -> Intermediate -> Mart)
5. Streamlit / Tableau (Visualization)

## CI/CD Orchestration
The entire data pipeline is fully automated using GitHub Actions:
- Scheduled Sync: A workflow runs every day at 00:00 UTC.
- Automated Extraction: Triggers the Python script to fetch incremental data from yfinance.
- dbt Execution: Runs dbt deps and dbt run automatically inside the GitHub runner after extraction.
- Secure Authentication: Uses Google Service Account via GitHub Secrets with direct memory injection for secure and reliable runs.

---

## dbt Layered Modeling

## Staging Layer
- stg_prices
- stg_metadata

Standardized raw financial data.

## Intermediate Layer
- i_returns
- i_cumulative
- i_volatility
- i_drawdown
- i_sharpe
- i_cagr

Financial risk and return logic:
- Log-compounded cumulative returns
- Rolling annualized volatility
- Period-based Sharpe Ratio
- Peak-to-trough drawdown calculation

## Mart Layer
- mart_prices

Final analytics table consolidating:
- Price
- Returns
- Risk metrics
- Company metadata

---

# Financial Metrics Implemented

## Sharpe Ratio
Period-based, annualized:
(mean excess return / std deviation) x sqrt(252)

## Volatility
Realized volatility of selected period:
std(daily returns) x sqrt(252)

## Max Drawdown
Peak-to-trough decline within selected time window.

## CAGR
Compound annual growth rate:
(final / initial)^(1/years) - 1

## Cumulative Return
Log-compounded:
exp(sum(ln(1 + daily_return))) - 1

---

# Tech Stack

| Layer | Technology |
|--------|------------|
| Orchestration | GitHub Actions |
| Data Extraction | Python, yfinance |
| Data Warehouse | Google BigQuery |
| Transformations | dbt Core |
| Analytics | SQL, Financial Modeling |
| Dashboard | Streamlit |
| BI | Tableau |

---

# Technical Roadmap

- [ ] Orchestration: Implement GitHub Actions to automate the daily extraction and dbt run.
- [ ] Incremental Loading: Transition dbt models to incremental materialization for performance.
- [ ] Unit Testing: Add dbt data tests for financial threshold validation.

# Project Objective
This project simulates a production-grade environment, integrating Data Engineering, Analytics Engineering, and Financial Modeling. It is designed to demonstrate proficiency in cloud-based data stacks (GCP) and the separation of concerns between data transformation and visualization.

# Author: Antuel Quirino