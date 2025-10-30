import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime

# ===== CONFIG =====
EMA_PERIOD = 50
OBI_PERIOD = 5
OBI_THRESHOLD = 0.25
TP_PCT = 0.006
SL_PCT = 0.003
FEE = 0.0005
LEVERAGE = 50
MARGIN_PER_TRADE = 1.5
COOLDOWN_CANDLES = 1
LIMIT = 2000  # max klines per request
INTERVAL = "5m"  # timeframe


# ===== INDICATORS =====
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_obi(df):
    body = df["close"] - df["open"]
    rng = df["high"] - df["low"]
    ratio = np.where(rng == 0, 0, body / rng)
    return pd.Series(ratio).rolling(OBI_PERIOD).mean()


# ===== FETCH DATA =====
def fetch_klines_full_month(symbol: str, interval="1m", days=10):
    url = "https://fapi.binance.com/fapi/v1/klines"
    end_time = int(time.time() * 1000)  # current timestamp in ms
    start_time = end_time - days * 24 * 60 * 60 * 1000
    all_data = []

    while start_time < end_time:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": 1000,
            "startTime": start_time
        }
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print(f"❌ Error fetching {symbol}: {r.text}")
            break
        data = r.json()
        if not data:
            break

        all_data.extend(data)
        # move to next chunk
        last_close_time = data[-1][6]
        start_time = last_close_time + 1
        time.sleep(0.2)  # to respect API rate limits

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_base_vol", "taker_quote_vol", "ignore"
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")  # ✅ timestamp

    print(f"✅ {symbol}: fetched {len(df)} candles (~{len(df)/1440:.1f} days)")
    return df


# ===== STRATEGY =====
def backtest_strategy(df, initial_balance=10.0):
    df = df.copy()
    df["ema"] = compute_ema(df["close"], EMA_PERIOD)
    df["obi"] = compute_obi(df)
    df["signal"] = np.where(
        (df["obi"] > OBI_THRESHOLD) & (df["close"] > df["ema"]), "long",
        np.where((df["obi"] < -OBI_THRESHOLD) & (df["close"] < df["ema"]), "short", None)
    )

    trades = []
    cooldown = 0
    active_trade = None
    balance = initial_balance

    for i in range(len(df)):
        if cooldown > 0:
            cooldown -= 1
            continue
        row = df.iloc[i]

        # manage open position
        if active_trade:
            if active_trade["direction"] == "long":
                hit_tp = row["high"] >= active_trade["tp"]
                hit_sl = row["low"] <= active_trade["sl"]
            else:
                hit_tp = row["low"] <= active_trade["tp"]
                hit_sl = row["high"] >= active_trade["sl"]

            if hit_tp or hit_sl:
                exit_price = active_trade["tp"] if hit_tp else active_trade["sl"]
                pnl_pct = (exit_price - active_trade["entry"]) / active_trade["entry"]
                if active_trade["direction"] == "short":
                    pnl_pct = -pnl_pct
                pnl_pct = pnl_pct * LEVERAGE - FEE
                pnl_usdt = MARGIN_PER_TRADE * pnl_pct
                balance += pnl_usdt
                trades.append({
                    "entry_idx": active_trade["entry_idx"],
                    "exit_idx": i,
                    "entry": active_trade["entry"],
                    "exit": exit_price,
                    "direction": active_trade["direction"],
                    "pnl_usdt": pnl_usdt,
                    "balance_after": balance,
                    "entry_date": df["open_time"].iloc[active_trade["entry_idx"]],
                    "exit_date": df["open_time"].iloc[i]
                })
                active_trade = None
                cooldown = COOLDOWN_CANDLES
                continue

        # open new position
        if active_trade is None and row["signal"] in ("long", "short"):
            direction = row["signal"]
            entry = row["close"]
            if direction == "long":
                tp = entry * (1 + TP_PCT)
                sl = entry * (1 - SL_PCT)
            else:
                tp = entry * (1 - TP_PCT)
                sl = entry * (1 + SL_PCT)
            active_trade = {
                "direction": direction,
                "entry": entry,
                "tp": tp,
                "sl": sl,
                "entry_idx": i
            }

    # close remaining trade at end
    if active_trade:
        final_close = df["close"].iloc[-1]
        pnl_pct = (final_close - active_trade["entry"]) / active_trade["entry"]
        if active_trade["direction"] == "short":
            pnl_pct = -pnl_pct
        pnl_pct = pnl_pct * LEVERAGE - FEE
        pnl_usdt = MARGIN_PER_TRADE * pnl_pct
        balance += pnl_usdt
        trades.append({
            "entry_idx": active_trade["entry_idx"],
            "exit_idx": len(df) - 1,
            "entry": active_trade["entry"],
            "exit": final_close,
            "direction": active_trade["direction"],
            "pnl_usdt": pnl_usdt,
            "balance_after": balance,
            "entry_date": df["open_time"].iloc[active_trade["entry_idx"]],
            "exit_date": df["open_time"].iloc[-1]
        })

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return trades_df, {
            "total_trades": 0, "total_pnl": 0,
            "win_rate": 0, "avg_pnl": 0,
            "initial_balance": initial_balance, "final_balance": initial_balance
        }

    total_pnl = trades_df["pnl_usdt"].sum()
    win_rate = (trades_df["pnl_usdt"] > 0).mean() * 100
    avg_pnl = trades_df["pnl_usdt"].mean()

    summary = {
        "total_trades": len(trades_df),
        "win_rate": round(win_rate, 2),
        "avg_pnl": round(avg_pnl, 4),
        "total_pnl": round(total_pnl, 2),
        "initial_balance": round(initial_balance, 2),
        "final_balance": round(balance, 2)
    }
    return trades_df, summary

# ===== MULTI-SYMBOL BACKTEST =====
def backtest_symbols(symbols):
    all_summaries = []
    for sym in symbols:
        print(f"📡 Fetching {sym} ...")
        df = fetch_klines_full_month(sym, INTERVAL, days=5)  # ✅ fixed param
        print(f"Fetched {len(df)} candles for {sym}")
        if df is None or df.empty:
            continue

        trades, summary = backtest_strategy(df)
        summary["symbol"] = sym
        all_summaries.append(summary)

        if not trades.empty:
            trades.to_csv(f"./trade_data/trades_{sym}.csv", index=False)
            print(f"💾 Saved trades_{sym}.csv")

        print(f"✅ {sym} done → {summary}")
        time.sleep(0.5)
    summary_df = pd.DataFrame(all_summaries)
    summary_df.to_csv("virtuum_backtest_results.csv", index=False)
    print("💾 Saved virtuum_backtest_results.csv")
    return summary_df


# ===== MAIN =====
if __name__ == "__main__":
    symbols = ["4USDT", "AIAUSDT", "BANKUSDT", "CAKEUSDT", "COAIUSDT",
            "FORMUSDT", "LISTUSDT", "LYNUSDT", "TAKEUSDT", "USELESSUSDT", "VVVUSDT"]
    results = backtest_symbols(symbols)
    print("\n📊 Summary Table:")
    print(results)
    results.to_csv("virtuum_backtest_results.csv", index=False)