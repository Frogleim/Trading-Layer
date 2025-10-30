import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from binance.client import Client
from binance.enums import HistoricalKlinesType

# ===== CONFIG =====
API_KEY = "YOUR_BINANCE_API_KEY"
API_SECRET = "YOUR_BINANCE_API_SECRET"
client = Client(API_KEY, API_SECRET)

INTERVAL = Client.KLINE_INTERVAL_5MINUTE
SAVE_PATH = "/Volumes/MyDrive/"
SLEEP_BETWEEN_CALLS = 0.3  # seconds

# ===== INDICATORS =====
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_obi(df, period=5):
    body = df["close"] - df["open"]
    rng = df["high"] - df["low"]
    ratio = np.where(rng == 0, 0, body / rng)
    return pd.Series(ratio).rolling(period).mean()

# ===== FETCH FULL RANGE =====
def fetch_futures_klines_full(symbol: str, start_date: datetime, end_date: datetime, interval="5m"):
    all_klines = []
    start = start_date

    while start < end_date:
        end = start + timedelta(days=10)  # each chunk ≈ 10 days (2880 candles)
        if end > end_date:
            end = end_date

        try:
            klines = client.futures_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start.strftime("%Y-%m-%d %H:%M:%S"),
                end_str=end.strftime("%Y-%m-%d %H:%M:%S"),
                limit=1500
            )
        except Exception as e:
            print(f"❌ {symbol} error: {e}")
            time.sleep(2)
            continue

        if not klines:
            print(f"⚠️ No data for {symbol} {start:%Y-%m-%d}")
            start = end
            continue

        all_klines.extend(klines)
        print(f"📈 {symbol}: {len(klines)} rows from {start:%Y-%m-%d} → {end:%Y-%m-%d}")
        start = end
        time.sleep(SLEEP_BETWEEN_CALLS)

    df = pd.DataFrame(all_klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_base_vol", "taker_quote_vol", "ignore"
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

# ===== MAIN DOWNLOADER =====
def download_2years(symbols):
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=360)  # ~2 years

    for sym in symbols:
        print(f"\n🚀 Downloading {sym} (2 years of {INTERVAL})")
        df = fetch_futures_klines_full(sym, start_date, end_date, INTERVAL)
        if df.empty:
            print(f"⚠️ No data for {sym}")
            continue

        # indicators (optional for your backtester)
        df["ema50"] = compute_ema(df["close"], 50)
        df["obi5"] = compute_obi(df, period=5)

        path = f"{SAVE_PATH}{sym}_5m_2y.csv"
        df.to_csv(path, index=False)
        print(f"💾 Saved {path} | {len(df)} rows\n")
        time.sleep(1)

    print("✅ All symbols done.")

# ===== RUN =====
if __name__ == "__main__":
    symbols = [ "LYNUSDT", "TAKEUSDT", "USELESSUSDT", "VVVUSDT"]
    download_2years(symbols)
