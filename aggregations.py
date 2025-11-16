# aggregations.py

import pandas as pd
from pathlib import Path

CLEANED_PARQUET_PATH = Path("data/cleaned.parquet")


def load_cleaned() -> pd.DataFrame:
    if not CLEANED_PARQUET_PATH.exists():
        raise FileNotFoundError("cleaned.parquet not found. Run data_cleaning.py first.")
    return pd.read_parquet(CLEANED_PARQUET_PATH)


# 1️⃣ Daily average close price by ticker
def agg_daily_avg_close(df: pd.DataFrame) -> pd.DataFrame:
    if "close_price" not in df.columns:
        raise ValueError("Expected 'close_price' column for daily avg close aggregation.")

    agg = (
        df.groupby(["date", "ticker"], dropna=True)["close_price"]
          .mean()
          .reset_index()
          .rename(columns={"close_price": "avg_close_price"})
    )
    return agg


# 2️⃣ Average volume by sector
def agg_avg_volume_by_sector(df: pd.DataFrame) -> pd.DataFrame:
    if "volume" not in df.columns:
        raise ValueError("Expected 'volume' column for volume aggregation.")

    if "sector" not in df.columns:
        raise ValueError("Expected 'sector' column for sector-volume aggregation.")

    agg = (
        df.groupby("sector")["volume"]
          .mean()
          .reset_index()
          .rename(columns={"volume": "avg_volume"})
    )
    return agg


# 3️⃣ Simple daily return by ticker: (close - open) / open
def agg_daily_returns(df: pd.DataFrame) -> pd.DataFrame:
    if "open_price" not in df.columns or "close_price" not in df.columns:
        raise ValueError("Expected open_price & close_price for returns calculation.")

    df = df.copy()
    df["daily_return"] = (df["close_price"] - df["open_price"]) / df["open_price"]

    return df[["date", "ticker", "daily_return"]]


def main():
    df = load_cleaned()

    # Run aggregations
    agg1 = agg_daily_avg_close(df)
    agg2 = agg_avg_volume_by_sector(df)
    agg3 = agg_daily_returns(df)

    # Save outputs
    agg1.to_parquet("data/agg_daily_avg_close.parquet", index=False)
    agg2.to_parquet("data/agg_avg_volume_by_sector.parquet", index=False)
    agg3.to_parquet("data/agg_daily_returns.parquet", index=False)

    print("Saved: ")
    print(" - data/agg_daily_avg_close.parquet")
    print(" - data/agg_avg_volume_by_sector.parquet")
    print(" - data/agg_daily_returns.parquet")


if __name__ == "__main__":
    main()
