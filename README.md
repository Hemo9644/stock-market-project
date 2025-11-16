# Stock Market Data Pipeline & Streamlit Dashboard

This project demonstrates a full **ETL + Aggregation + Visualization** workflow using Python, Pandas, Parquet, and Streamlit.  
It includes:

- Data Cleaning (CSV → cleaned.parquet)
- Aggregations (daily close, volume by sector, daily returns)
- Interactive dashboard with filters

---

## 📁 Project Structure


stock-market-project/
│
├── data/
│ ├── stock_market.csv
│ ├── cleaned.parquet
│ ├── agg_daily_avg_close.parquet
│ ├── agg_avg_volume_by_sector.parquet
│ ├── agg_daily_returns.parquet
│
├── data_cleaning.py
├── aggregations.py
├── app.py
├── pyproject.toml
├── README.md
└── .venv/


---

## 🧹 1. Data Cleaning (data_cleaning.py)

### Steps performed:
- Load raw CSV
- Convert column names → **snake_case**
- Strip whitespace
- Normalize null values (NA, N/A, -, null, etc.)
- Convert `"Trade Date"` → `date` (ISO `YYYY-MM-DD`)
- Standardize text case (ticker → uppercase, sector → title case)
- Convert numeric columns (`open_price`, `close_price`, `volume`)
- Drop invalid rows
- Deduplicate all rows
- Save output as **Parquet**

### Run:

```powershell
uv run python data_cleaning.py


Output file:

data/cleaned.parquet

📊 2. Aggregations (aggregations.py)

Generated aggregations:

Daily average close price by ticker

Average volume by sector

Daily return: (close_price - open_price) / open_price

Run:
uv run python aggregations.py


Output files:

data/agg_daily_avg_close.parquet
data/agg_avg_volume_by_sector.parquet
data/agg_daily_returns.parquet

📺 3. Streamlit Dashboard (app.py)

Interactive dashboard features:

Date range filter

Ticker dropdown filter

Charts built using Altair

Line chart: daily average close

Line chart: daily returns

Bar chart: volume by sector

Data tables for all parquet outputs

Run:
uv run streamlit run app.py


Open your browser at:

http://localhost:8501

📸 Required Deliverables for Submission

Include in your GitHub repository:

data_cleaning.py

aggregations.py

app.py

All .parquet files inside /data

The stock_market.csv

This README.md

3–5 screenshots of:

Your Streamlit dashboard home

Daily close chart

Volume by sector bar chart

Daily returns chart

Filter panel

🛠 Development Instructions
Setup environment (UV)
uv venv
uv pip install pandas pyarrow streamlit altair

