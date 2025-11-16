# app.py
import streamlit as st
import pandas as pd
from pathlib import Path
import altair as alt

DATA_DIR = Path("data")

FILE_DAILY_CLOSE   = DATA_DIR / "agg_daily_avg_close.parquet"
FILE_SECTOR_VOLUME = DATA_DIR / "agg_avg_volume_by_sector.parquet"
FILE_DAILY_RET     = DATA_DIR / "agg_daily_returns.parquet"

def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        st.error(f"File not found: {path}. Run data_cleaning.py and aggregations.py first.")
        st.stop()
    return pd.read_parquet(path)


def sidebar_filters(df):
    st.sidebar.header("Filters")

    # Date Range
    min_date = df["date"].min()
    max_date = df["date"].max()

    selected_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Ensure tuple
    if isinstance(selected_range, tuple):
        start, end = selected_range
    else:
        start, end = min_date, max_date

    # Ticker filter
    tickers = ["ALL"] + sorted(df["ticker"].dropna().unique().tolist())
    selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)

    # Apply filters
    df_filtered = df[(df["date"] >= pd.to_datetime(start)) & 
                     (df["date"] <= pd.to_datetime(end))]
    
    if selected_ticker != "ALL":
        df_filtered = df_filtered[df_filtered["ticker"] == selected_ticker]

    return df_filtered


def chart_daily_close(df):
    st.subheader("📈 Daily Average Close Price")

    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="date:T",
            y="avg_close_price:Q",
            color="ticker:N"
        )
        .properties(height=350)
    )

    st.altair_chart(chart, use_container_width=True)


def chart_sector_volume(df):
    st.subheader("📊 Average Volume by Sector")

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="sector:N",
            y="avg_volume:Q",
            color="sector:N",
        )
        .properties(height=350)
    )

    st.altair_chart(chart, use_container_width=True)


def chart_daily_returns(df):
    st.subheader("📉 Daily Return by Ticker")

    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="date:T",
            y="daily_return:Q",
            color="ticker:N"
        )
        .properties(height=350)
    )

    st.altair_chart(chart, use_container_width=True)


def main():
    st.title("📊 Stock Market Dashboard")
    st.write("Interactive charts based on your cleaned and aggregated parquet files.")

    # Load aggregated datasets
    df_close = load_parquet(FILE_DAILY_CLOSE)
    df_volume = load_parquet(FILE_SECTOR_VOLUME)
    df_returns = load_parquet(FILE_DAILY_RET)

    # Filters apply only to date+ticker tables
    df_filtered = sidebar_filters(df_close)

    # Charts
    chart_daily_close(df_filtered)
    chart_daily_returns(df_returns)

    # Sector chart uses full dataset
    chart_sector_volume(df_volume)

    # Data Tables
    st.subheader("Filtered Daily Close Table")
    st.dataframe(df_filtered)

    st.subheader("Sector Volume Table")
    st.dataframe(df_volume)

    st.subheader("Daily Returns Table")
    st.dataframe(df_returns)


if __name__ == "__main__":
    main()
