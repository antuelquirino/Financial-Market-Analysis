import streamlit as st
from queries import get_tickers, get_ticker_data
from datetime import datetime, timedelta
import pandas as pd
import altair as alt
import numpy as np

# -------------------------------------------------------
# Layout
# -------------------------------------------------------
st.set_page_config(
    page_title="Financial Market Analysis",
    layout="wide",
)

st.markdown("""
<style>
.metric-label { font-size: 14px; opacity: 0.75; }
.metric-value { font-size: 28px; font-weight:600; }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------
# Sidebar
# -------------------------------------------------------
st.sidebar.title("Financial Market Analysis")

tickers = get_tickers()
ticker = st.sidebar.selectbox("Ticker", tickers)

range_opt = st.sidebar.selectbox("Range", ["1M", "6M", "1Y", "3Y", "5Y"])
benchmark = "^GSPC"

# -------------------------------------------------------
# Range logic
# -------------------------------------------------------
today = datetime.today()
ranges = {
    "1M": today - timedelta(days=30),
    "6M": today - timedelta(days=180),
    "1Y": today - timedelta(days=365),
    "3Y": today - timedelta(days=365*3),
    "5Y": today - timedelta(days=365*5),
}

start_date = ranges[range_opt].strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")

# -------------------------------------------------------
# Fetch data
# -------------------------------------------------------
df = get_ticker_data(ticker, start_date=start_date, end_date=end_date)
df_bench = get_ticker_data(benchmark, start_date=start_date, end_date=end_date)

if df.empty or df_bench.empty:
    st.error("No data available for this period.")
    st.stop()

# -------------------------------------------------------
# Processing
# -------------------------------------------------------
df['date'] = pd.to_datetime(df['date'])
df_bench['date'] = pd.to_datetime(df_bench['date'])

df = df.set_index('date').sort_index()
df_bench = df_bench.set_index('date').sort_index()
df_bench = df_bench.reindex(df.index, method='ffill')

returns = df['daily_return'].dropna()

# -------------------------------------------------------
# KPIs – PERIOD CORRECT (FINAL)
# -------------------------------------------------------
# Sharpe Ratio (period-based, annualized)
risk_free_annual = 0.04
risk_free_daily = risk_free_annual / 252

excess_returns = returns - risk_free_daily
sharpe = np.nan
if excess_returns.std() != 0:
    sharpe = (excess_returns.mean() / excess_returns.std()) * np.sqrt(252)

# Volatility KPI (REALIZED volatility of selected period)
vol_value = returns.std() * np.sqrt(252)
vol_label = "Volatility (annualized)"

# Max Drawdown (period-based)
cum = (1 + returns).cumprod()
running_max = cum.cummax()
drawdown = ((cum - running_max) / running_max).min()

# Return KPI
if range_opt == "1M":
    return_label = "Period Return"
    return_value = (df['price'].iloc[-1] / df['price'].iloc[0]) - 1
else:
    return_label = "CAGR"
    days = (df.index[-1] - df.index[0]).days
    years = max(days / 365, 0.01)
    return_value = (df['price'].iloc[-1] / df['price'].iloc[0]) ** (1 / years) - 1

# -------------------------------------------------------
# Rolling Volatility (Chart)
# -------------------------------------------------------
rolling_window = min(30, len(returns))
vol_chart = (
    returns
    .rolling(window=rolling_window, min_periods=5)
    .std()
    * np.sqrt(252)
)

# -------------------------------------------------------
# Title
# -------------------------------------------------------
st.markdown(f"### {ticker} — Performance Dashboard")

# -------------------------------------------------------
# KPIs
# -------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

col1.markdown(
    f"<div class='metric-label'>Sharpe Ratio</div>"
    f"<div class='metric-value'>{sharpe:.2f}</div>",
    unsafe_allow_html=True
)

col2.markdown(
    f"<div class='metric-label'>{vol_label}</div>"
    f"<div class='metric-value'>{vol_value*100:.2f}%</div>",
    unsafe_allow_html=True
)

col3.markdown(
    f"<div class='metric-label'>Max Drawdown</div>"
    f"<div class='metric-value'>{drawdown*100:.2f}%</div>",
    unsafe_allow_html=True
)

col4.markdown(
    f"<div class='metric-label'>{return_label}</div>"
    f"<div class='metric-value'>{return_value*100:.2f}%</div>",
    unsafe_allow_html=True
)

# -------------------------------------------------------
# Tabs
# -------------------------------------------------------
tabs = st.tabs([
    "Price",
    "Cumulative Return",
    "Daily Return",
    "Drawdown",
    "Volatility"
])

# ---------- PRICE ----------
with tabs[0]:
    chart = alt.Chart(df.reset_index()).mark_line().encode(
        x='date:T',
        y=alt.Y('price:Q', title='Price (USD)'),
        tooltip=['date:T', alt.Tooltip('price:Q', format=',.2f')]
    ).properties(height=260)

    st.altair_chart(chart, use_container_width=True)

# ---------- CUMULATIVE RETURN ----------
with tabs[1]:
    tmp = pd.DataFrame({
        "date": df.index,
        ticker: (1 + df['daily_return']).cumprod() - 1,
        benchmark: (1 + df_bench['daily_return']).cumprod() - 1
    }).reset_index(drop=True)

    tmp = tmp.melt('date', var_name='Asset', value_name='cum')

    chart = alt.Chart(tmp).mark_line().encode(
        x='date:T',
        y=alt.Y('cum:Q', axis=alt.Axis(format='%'), title='Cumulative Return'),
        color='Asset:N',
        tooltip=['date:T', 'Asset:N', alt.Tooltip('cum:Q', format='.2%')]
    ).properties(height=260)

    st.altair_chart(chart, use_container_width=True)

# ---------- DAILY RETURN ----------
with tabs[2]:
    base = alt.Chart(df.reset_index()).encode(
        x='date:T',
        y=alt.Y('daily_return:Q', axis=alt.Axis(format='%'), title='Daily Return'),
        tooltip=['date:T', alt.Tooltip('daily_return:Q', format='.2%')]
    )

    pos = base.transform_filter(alt.datum.daily_return >= 0).mark_area(color='lightblue')
    neg = base.transform_filter(alt.datum.daily_return < 0).mark_area(color='#8B0000')

    zero = alt.Chart(pd.DataFrame({'y':[0]})).mark_rule(stroke='white').encode(y='y:Q')

    st.altair_chart((pos + neg + zero).properties(height=260), use_container_width=True)

# ---------- DRAWDOWN ----------
with tabs[3]:
    dd_df = pd.DataFrame({
        "date": cum.index,
        "drawdown": (cum - running_max) / running_max
    })

    chart = alt.Chart(dd_df).mark_area(
        line={'color': '#8B0000', 'width': 1.5},
        color='lightblue'
    ).encode(
        x='date:T',
        y=alt.Y('drawdown:Q', axis=alt.Axis(format='%'), title='Drawdown'),
        tooltip=['date:T', alt.Tooltip('drawdown:Q', format='.2%')]
    ).properties(height=260)

    st.altair_chart(chart, use_container_width=True)

# ---------- VOLATILITY ----------
with tabs[4]:
    vol_df = pd.DataFrame({
        "date": vol_chart.index,
        "vol": vol_chart
    }).dropna()

    chart = alt.Chart(vol_df).mark_line().encode(
        x='date:T',
        y=alt.Y('vol:Q', axis=alt.Axis(format='%'), title="Rolling Volatility (30D, annualized)"),
        tooltip=['date:T', alt.Tooltip('vol:Q', format='.2%')]
    ).properties(height=260)

    st.altair_chart(chart, use_container_width=True)


