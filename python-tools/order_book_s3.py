import os
import json
import boto3
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from tardis_dev import datasets

# === AWS Setup ===
AWS_ACCESS_KEY_ID = "AKIAQQTAXY3PFDP4JINF"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "eu-central-1"
BUCKET_NAME = "data-order-book"
TARDIS_API = "TD.klFtkVwVJI6iFomC.NaelfCtfEbkKYcc.SPbFwEy7MuG021F.RrhjPJQuKjA91dK.OFPYuc5uHN7PqFD.6MPX"

# Initialize S3 client
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)
s3 = session.client("s3")

# Create temp download dir
DOWNLOAD_DIR = Path("temp_data")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Load coin data
with open('final_coin_info.json', 'r') as f:
    final_coin_info = json.load(f)

# === Worker Function ===
def process_coin(coin):
    symbol = coin['id']
    try:
        # Download data
        datasets.download(
            exchange="binance-futures",
            data_types=["incremental_book_L2", "trades"],
            from_date=coin['first_date'],
            to_date=coin['last_date'],
            symbols=[symbol],
            api_key=TARDIS_API,
            download_dir=DOWNLOAD_DIR
        )

        # Upload to S3
        for file in DOWNLOAD_DIR.glob("*"):
            s3.upload_file(
                Filename=str(file),
                Bucket=BUCKET_NAME,
                Key=f"{symbol}/{file.name}",
                ExtraArgs={"ContentType": "text/csv"},
            )

        # Cleanup
        for file in DOWNLOAD_DIR.glob("*"):
            file.unlink()

        return f"✅ Done: {symbol}"

    except Exception as e:
        return f"❌ Failed {symbol}: {e}"

# === Parallel execution with proper tqdm update ===
MAX_WORKERS = 7
futures = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for coin in final_coin_info[:114]:
        futures.append(executor.submit(process_coin, coin))

    with tqdm(total=len(futures), desc="Processing coins") as pbar:
        for future in as_completed(futures):
            result = future.result()
            print(result)
            pbar.update(1)
