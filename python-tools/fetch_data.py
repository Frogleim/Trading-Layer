import pandas as pd
import requests
import time

def fetch_klines_range(symbol, start, end, interval="3m"):
    url = "https://fapi.binance.com/fapi/v1/klines"
    start_ms = int(pd.Timestamp(start).timestamp() * 1000)
    end_ms = int(pd.Timestamp(end).timestamp() * 1000)

    all_rows = []

    while start_ms < end_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": 1000,
            "startTime": start_ms,
            "endTime": end_ms
        }

        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not data:
            break

        all_rows.extend(data)
        start_ms = data[-1][6] + 1
        time.sleep(0.2)

    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time","q","n","tb","tq","ignore"
    ])

    df = df[["open_time","open","high","low","close","volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time")

    return df