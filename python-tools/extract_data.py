import gzip
import shutil
from pathlib import Path
import pandas as pd
# Path to your compressed file


def extract_data():
    compressed_file = Path("downloads/{symbol}/binance-futures_incremental_book_L2_2025-04-08_JELLYJELLYUSDT.csv.gz")
    output_file = compressed_file.with_suffix('')  # removes .gz → creates .csv

    # Extract
    with gzip.open(compressed_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"✅ Extracted to {output_file}")



df = pd.read_csv(
    "./downloads/JELLYJELLYUSDT/binance-futures_incremental_book_L2_2025-04-08_JELLYJELLYUSDT.csv",
    usecols=["timestamp", "side", "price", "amount"]
)
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")

def compute_obi(group):
    bids = group[group["side"].values == "bid"]
    asks = group[group["side"].values == "ask"]

    if bids.empty or asks.empty:
        return None

    bid_vol = bids.nlargest(10, "price")["amount"].sum()
    ask_vol = asks.nsmallest(10, "price")["amount"].sum()

    denom = bid_vol + ask_vol
    return (bid_vol - ask_vol) / denom if denom > 0 else None

obi_df = (
    df.groupby("timestamp", sort=False, observed=True)
    .apply(compute_obi)
    .dropna()
    .reset_index(name="obi")
)

print(obi_df.head())