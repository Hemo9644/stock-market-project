# data_cleaning.py

import re
from pathlib import Path
import pandas as pd

# ✔ Correct paths
RAW_CSV_PATH = Path("data/stock_market.csv")
CLEANED_PARQUET_PATH = Path("data/cleaned.parquet")


def to_snake_case(col: str) -> str:
    """Convert a column name to snake_case and trim whitespace."""
    col = col.strip()
    col = col.lower()
    col = re.sub(r"[^0-9a-zA-Z]+", "_", col)  # non-alphanumeric → underscore
    col = re.sub(r"_{2,}", "_", col)          # collapse multiple underscores
    col = col.strip("_")
    return col


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [to_snake_case(c) for c in df.columns]
    return df


def load_raw_csv() -> pd.DataFrame:
    na_values = ["", "NA", "N/A", "null", "Null", "NULL", "-"]

    df = pd.read_csv(
        RAW_CSV_PATH,
        na_values=na_values,
        keep_default_na=True,
    )
    print("Raw shape:", df.shape)
    print(df.head())
    print(df.info())
    print(df.isna().sum())
    return df


def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace in all string columns."""
    df = df.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def standardize_text_case(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize case for columns like ticker, sector."""
    df = df.copy()

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].str.upper()

    if "sector" in df.columns:
        df["sector"] = df["sector"].str.title()

    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse Trade Date → trade_date → date (yyyy-MM-dd)
    """
    df = df.copy()

    if "trade_date" not in df.columns:
        raise ValueError("Expected 'trade_date' after header normalization.")

    df["trade_date"] = (
        df["trade_date"].astype(str).str.strip().replace({"NaT": None})
    )

    df["trade_date"] = pd.to_datetime(
        df["trade_date"], errors="coerce", format="%m/%d/%Y"
    )

    df = df.dropna(subset=["trade_date"])

    df = df.rename(columns={"trade_date": "date"})

    return df


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Your dataset has: open_price, close_price, volume
    numeric_cols = [c for c in ["open_price", "close_price", "volume"] if c in df.columns]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    print(f"Deduplicated rows: {before - after}")
    return df


def main():
    if not RAW_CSV_PATH.exists():
        raise FileNotFoundError(f"{RAW_CSV_PATH} does not exist. Put stock_market.csv inside /data")

    df = load_raw_csv()
    df = normalize_headers(df)
    df = strip_string_columns(df)
    df = standardize_text_case(df)
    df = parse_dates(df)
    df = enforce_schema(df)
    df = deduplicate(df)

    print("Cleaned shape:", df.shape)
    print(df.head())

    CLEANED_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CLEANED_PARQUET_PATH, index=False)
    print(f"Saved cleaned data to {CLEANED_PARQUET_PATH}")


if __name__ == "__main__":
    main()
