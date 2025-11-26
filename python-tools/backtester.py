import pandas as pd
import numpy as np
import time
from datetime import datetime
from fetch_data import fetch_klines_range

# ============================
# CONFIG
# ============================
EMA_PERIOD = 50
ATR_PERIOD = 14

OBI_THRESHOLD = 0.30  # confirmation filter
TP_ATR_MULT = 1.5
SL_ATR_MULT = 0.7

MARGIN_PER_TRADE = 20
LEVERAGE = 50
FEE = 0.0005

COOLDOWN = 1


# ============================
# INDICATORS
# ============================
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_atr(df, n=14):
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(n).mean()


# ============================
# REAL ORDER BOOK OBI
# ============================
def load_bookdepth(path):
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def compute_real_obi_from_bookdepth(df):
    bids = df[df["percentage"] < 0].groupby("timestamp")["notional"].sum()
    asks = df[df["percentage"] > 0].groupby("timestamp")["notional"].sum()

    obi_raw = (bids - asks) / (bids + asks)
    obi_smooth = obi_raw.ewm(alpha=0.2).mean()

    return obi_smooth.to_frame("obi")


def resample_obi_to_candles(obi_df, candle_df):
    obi_res = obi_df.resample("3min").last()
    merged = candle_df.merge(obi_res, left_on="open_time", right_index=True, how="left")
    merged["obi"] = merged["obi"].ffill().bfill()
    return merged


# ============================
# EXECUTION ENGINE
# ============================
def resolve_order(direction, open_p, high, low, tp, sl):
    """SL-before-TP realistic matching."""
    if direction == "long":
        if low <= sl and high >= tp:
            return sl if abs(open_p - sl) < abs(tp - open_p) else tp
        if low <= sl:
            return sl
        if high >= tp:
            return tp
        return None

    else:  # short
        if high >= sl and low <= tp:
            return sl if abs(open_p - sl) < abs(open_p - tp) else tp
        if high >= sl:
            return sl
        if low <= tp:
            return tp
        return None


# ============================
# STRATEGY
# ============================
def backtest_hybrid(df, initial_balance=785):
    df = df.copy()

    df["ema"] = compute_ema(df["close"], EMA_PERIOD)
    df["atr"] = compute_atr(df, ATR_PERIOD)

    df = df.dropna()

    # candle impulse direction
    df["impulse"] = np.where(df["close"] > df["open"], "up",
                             np.where(df["close"] < df["open"], "down", "flat"))

    trades = []
    active = None
    cooldown = 0
    balance = initial_balance

    for i in range(ATR_PERIOD + 1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # manage open trade
        if active:
            fill = resolve_order(
                active["direction"],
                row["open"],
                row["high"],
                row["low"],
                active["tp"],
                active["sl"]
            )

            if fill is not None:
                exit_price = fill

                if active["direction"] == "long":
                    pnl_pct = (exit_price - active["entry"]) / active["entry"]
                else:
                    pnl_pct = (active["entry"] - exit_price) / active["entry"]

                pnl = active["size"] * pnl_pct * LEVERAGE
                pnl -= active["size"] * FEE * 2
                balance += pnl

                trades.append({
                    "entry_idx": active["i"],
                    "exit_idx": i,
                    "entry": active["entry"],
                    "exit": exit_price,
                    "direction": active["direction"],
                    "pnl_usdt": pnl,
                    "balance_after": balance,
                    "entry_date": df["open_time"].iloc[active["i"]],
                    "exit_date": df["open_time"].iloc[i]
                })

                active = None
                cooldown = COOLDOWN
                continue

        if cooldown > 0:
            cooldown -= 1
            continue

        # ============================
        # ENTRY LOGIC (Hybrid)
        # ============================

        signal = None

        # Trend + volatility + impulse
        if prev["close"] > prev["ema"] and prev["atr"] > df["atr"].iloc[i - 2] and prev["impulse"] == "up":
            signal = "long"

        if prev["close"] < prev["ema"] and prev["atr"] > df["atr"].iloc[i - 2] and prev["impulse"] == "down":
            signal = "short"

        if signal is None:
            continue

        # Order book confirmation
        if signal == "long" and prev["obi"] < OBI_THRESHOLD:
            continue
        if signal == "short" and prev["obi"] > -OBI_THRESHOLD:
            continue

        # Position size limit
        if balance < MARGIN_PER_TRADE:
            continue

        size = MARGIN_PER_TRADE

        entry = row["open"]

        if signal == "long":
            tp = entry + prev["atr"] * TP_ATR_MULT
            sl = entry - prev["atr"] * SL_ATR_MULT
        else:
            tp = entry - prev["atr"] * TP_ATR_MULT
            sl = entry + prev["atr"] * SL_ATR_MULT

        active = {
            "direction": signal,
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "size": size,
            "i": i
        }

    return pd.DataFrame(trades), balance



symbols = ["COAIUSDT", "JELLYJELLYUSDT", "PIPPINUSDT"]

for sym in symbols:
    print(f"\n=== {sym} ===")
    df_kline = fetch_klines_range(
        sym,
        "2025-11-16 00:00:00",
        "2025-11-17 00:00:00"
    )  # your existing function
    df_depth = load_bookdepth(f"{sym}-bookDepth.csv")

    obi = compute_real_obi_from_bookdepth(df_depth)
    df_merged = resample_obi_to_candles(obi, df_kline)

    trades, final_balance = backtest_hybrid(df_merged)

    print("Final balance:", final_balance)
    trades.to_csv(f"trades_{sym}.csv", index=False)