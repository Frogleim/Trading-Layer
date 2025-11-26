import os
import requests
import zipfile
import pandas as pd
from io import BytesIO

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"


def download_monthly_klines(symbol, interval="3m", years=[2024, 2025], months=range(1,13)):
    out_path = f"/Volumes/MyDrive/Binance_Data/{symbol}"
    os.makedirs(out_path, exist_ok=True)  # do NOT wrap in try/except

    for year in years:
        for month in months:
            mm = f"{month:02d}"
            file_name = f"{symbol}-{interval}-{year}-{mm}.zip"
            csv_name  = f"{symbol}-{interval}-{year}-{mm}.csv"

            csv_path = os.path.join(out_path, csv_name)

            # -----------------------------------------
            # ✅ DUPLICATE CHECK — skip if file exists
            # -----------------------------------------
            if os.path.exists(csv_path):
                print(f"⏭️ Already exists, skipping: {csv_name}")
                continue
            # -----------------------------------------

            url = f"{BASE_URL}/{symbol}/{interval}/{file_name}"
            print(f"⬇️ Downloading {file_name} ...")

            r = requests.get(url)
            if r.status_code != 200:
                print(f"❌ Not found: {file_name}")
                continue

            # Extract ZIP
            z = zipfile.ZipFile(BytesIO(r.content))
            z.extract(csv_name, out_path)

            print(f"📁 Extracted {csv_name}")
if __name__ == '__main__':
    from main import get_all_tradable_symbols

    symbols = get_all_tradable_symbols()
    symbols = sorted(symbols)
    for sym in symbols:
        download_monthly_klines(sym)   # ✔ one symbol at a time