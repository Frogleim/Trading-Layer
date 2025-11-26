import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime

# ===== CONFIG =====
EMA_PERIOD = 50
OBI_PERIOD = 5
OBI_THRESHOLD = 0.25
TP_PCT = 0.004
SL_PCT = 0.002
FEE = 0.0005
LEVERAGE = 75
MARGIN_PER_TRADE = 78
COOLDOWN_CANDLES = 1
INTERVAL = "3m"
ATR_PERIOD = 14  # for volatility-based spread/slippage


# ===== INDICATORS =====
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_obi(df):
    body = df["close"] - df["open"]
    rng = df["high"] - df["low"]
    ratio = np.where(rng == 0, 0, body / rng)
    return pd.Series(ratio).rolling(OBI_PERIOD).mean()


def compute_atr(df, n=14):
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def read_csv(symbol):
    df = pd.read_csv(f"./temp_data/{symbol}-3m-merged.csv")
    df = df[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

# ===== FETCH DATA =====
def fetch_klines_full_month(symbol, interval="3m", days=30):
    url = "https://fapi.binance.com/fapi/v1/klines"
    end = int(time.time() * 1000)
    start = end - days * 24 * 60 * 60 * 1000

    all_data = []
    while start < end:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": 1000,
            "startTime": start
        }
        r = requests.get(url, params=params)
        if r.status_code != 200:
            break

        data = r.json()
        if not data:
            break

        all_data.extend(data)
        start = data[-1][6] + 1
        time.sleep(0.2)

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "q", "n", "tb", "tq", "ignore"
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df


# ===== REALISTIC EXECUTION ENGINE =====
def resolve_order(direction, open_p, high, low, tp, sl, spread):
    """
    Implements realistic SL-before-TP candle simulation.
    Spread already included in entry.
    """
    if direction == "long":
        # SL is closer?
        if low <= sl and high >= tp:
            # decide which occurs first: whichever is closer to open
            if abs(open_p - sl) < abs(tp - open_p):
                return sl
            else:
                return tp
        elif low <= sl:
            return sl
        elif high >= tp:
            return tp
        else:
            return None

    else:  # short
        if high >= sl and low <= tp:
            if abs(open_p - sl) < abs(open_p - tp):
                return sl
            else:
                return tp
        elif high >= sl:
            return sl
        elif low <= tp:
            return tp
        else:
            return None


# ===== STRATEGY =====
def backtest_strategy_improved(df, initial_balance=785.0):
    df = df.copy()
    df = df[df["volume"] > 0]
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time")

    # Indicators
    df["ema"] = compute_ema(df["close"], EMA_PERIOD)
    df["obi"] = compute_obi(df)
    df["atr"] = compute_atr(df, ATR_PERIOD)

    # Signals
    df["signal"] = np.where(
        (df["obi"] > OBI_THRESHOLD) & (df["close"] > df["ema"]), "long",
        np.where((df["obi"] < -OBI_THRESHOLD) & (df["close"] < df["ema"]), "short", None)
    )

    trades = []
    active = None
    cooldown = 0
    balance = initial_balance

    for i in range(ATR_PERIOD + 1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # compute spread & slippage
        atr = row["atr"]
        spread = atr * 0.20
        slippage = atr * 0.10

        # ==== MANAGE OPEN POSITION ====
        if active:
            fill = resolve_order(
                active["direction"],
                row["open"],
                row["high"],
                row["low"],
                active["tp"],
                active["sl"],
                spread
            )

            if fill is not None:
                # exit price includes spread/slippage
                exit_p = fill - spread if active["direction"] == "long" else fill + spread

                # pnl
                if active["direction"] == "long":
                    pnl_pct = (exit_p - active["entry"]) / active["entry"]
                else:
                    pnl_pct = (active["entry"] - exit_p) / active["entry"]

                pnl = active["size"] * pnl_pct * LEVERAGE
                pnl -= active["size"] * FEE * 2
                balance += pnl

                trades.append({
                    "entry_idx": active["i"],
                    "exit_idx": i,
                    "entry": active["entry"],
                    "exit": exit_p,
                    "direction": active["direction"],
                    "pnl_usdt": pnl,
                    "balance_after": balance,
                    "entry_date": df["open_time"].iloc[active["i"]],
                    "exit_date": df["open_time"].iloc[i]
                })

                active = None
                cooldown = COOLDOWN_CANDLES
                continue

        if cooldown > 0:
            cooldown -= 1
            continue

        # ==== NEW ENTRY ====
        signal = prev["signal"]
        if signal in ("long", "short") and active is None and balance >= MARGIN_PER_TRADE:

            size = min(balance * 0.1, MARGIN_PER_TRADE)

            if signal == "long":
                entry_p = row["open"] + spread + slippage
                tp = entry_p * (1 + TP_PCT)
                sl = entry_p * (1 - SL_PCT)
            else:
                entry_p = row["open"] - spread - slippage
                tp = entry_p * (1 - TP_PCT)
                sl = entry_p * (1 + SL_PCT)

            active = {
                "direction": signal,
                "entry": entry_p,
                "tp": tp,
                "sl": sl,
                "size": size,
                "i": i
            }

    return pd.DataFrame(trades), calculate_summary(trades, initial_balance, balance)


# ===== SUMMARY (unchanged) =====
def calculate_summary(trades, initial_balance, final_balance):
    if not trades:
        return {k: 0 for k in [
            "total_trades","win_rate","avg_pnl","total_pnl","initial_balance",
            "final_balance","net_roi","avg_win","avg_loss","profit_factor",
            "max_drawdown","expectancy","sharpe_ratio","long_trades",
            "short_trades","win_long","win_short","avg_duration_candles",
            "gross_profit","gross_loss"
        ]}

    df = pd.DataFrame(trades)

    total = len(df)
    wins = df[df.pnl_usdt > 0]
    losses = df[df.pnl_usdt < 0]

    win_rate = len(wins) / total * 100
    avg_pnl = df.pnl_usdt.mean()
    total_pnl = df.pnl_usdt.sum()

    avg_win = wins.pnl_usdt.mean() if len(wins) else 0
    avg_loss = losses.pnl_usdt.mean() if len(losses) else 0
    gross_profit = wins.pnl_usdt.sum()
    gross_loss = -losses.pnl_usdt.sum()
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.inf

    balances = [initial_balance] + df.balance_after.tolist()
    drawdown = (pd.Series(balances) - pd.Series(balances).cummax()) / pd.Series(balances).cummax() * 100
    max_dd = drawdown.min()

    long_trades = df[df.direction == "long"]
    short_trades = df[df.direction == "short"]

    expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

    returns = df.pnl_usdt / initial_balance
    sharpe = (returns.mean() / returns.std()) * np.sqrt(365*288) if len(returns)>1 else 0

    df["duration"] = df["exit_idx"] - df["entry_idx"]

    return {
        "total_trades": total,
        "win_rate": round(win_rate, 2),
        "avg_pnl": round(avg_pnl, 4),
        "total_pnl": round(total_pnl, 2),
        "initial_balance": initial_balance,
        "final_balance": round(final_balance, 2),
        "net_roi": round((final_balance - initial_balance) / initial_balance * 100, 2),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown": round(max_dd, 2),
        "expectancy": round(expectancy, 4),
        "sharpe_ratio": round(sharpe, 2),
        "long_trades": len(long_trades),
        "short_trades": len(short_trades),
        "win_long": round((long_trades.pnl_usdt>0).mean()*100,2) if len(long_trades) else 0,
        "win_short": round((short_trades.pnl_usdt>0).mean()*100,2) if len(short_trades) else 0,
        "avg_duration_candles": round(df.duration.mean(),2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(-gross_loss, 2)
    }

def print_detailed_summary(summary, symbol=""):
    """Print a comprehensive strategy performance report"""
    print(f"\n{'='*60}")
    print(f"📊 STRATEGY PERFORMANCE REPORT: {symbol}")
    print(f"{'='*60}")

    print(f"💰 Balance & ROI:")
    print(f"   Initial Balance: ${summary['initial_balance']:,.2f}")
    print(f"   Final Balance:   ${summary['final_balance']:,.2f}")
    print(f"   Net PnL:        ${summary['total_pnl']:,.2f}")
    print(f"   ROI:            {summary['net_roi']:+.2f}%")

    print(f"\n📈 Trade Statistics:")
    print(f"   Total Trades:    {summary['total_trades']}")
    print(f"   Win Rate:        {summary['win_rate']:.2f}%")
    print(f"   Long Trades:     {summary['long_trades']} ({summary['win_long']:.1f}% win rate)")
    print(f"   Short Trades:    {summary['short_trades']} ({summary['win_short']:.1f}% win rate)")
    print(f"   Avg Duration:    {summary['avg_duration_candles']:.1f} candles")

    print(f"\n🎯 Performance Metrics:")
    print(f"   Avg PnL/Trade:  ${summary['avg_pnl']:+.4f}")
    print(f"   Avg Win:        ${summary['avg_win']:+.4f}")
    print(f"   Avg Loss:       ${summary['avg_loss']:+.4f}")
    print(f"   Expectancy:     ${summary['expectancy']:+.4f}")
    print(f"   Profit Factor:  {summary['profit_factor']:.2f}")
    print(f"   Sharpe Ratio:   {summary['sharpe_ratio']:.2f}")
    print(f"   Max Drawdown:   {summary['max_drawdown']:.2f}%")

    print(f"\n💵 PnL Breakdown:")
    print(f"   Gross Profit:   ${summary['gross_profit']:,.2f}")
    print(f"   Gross Loss:     ${summary['gross_loss']:,.2f}")
    print(f"{'='*60}")

# ===== FETCH ALL BINANCE FUTURES SYMBOLS =====
def get_all_tradable_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        symbols = [
            s["symbol"]
            for s in data["symbols"]
            if s["status"] == "TRADING"
               and s["contractType"] == "PERPETUAL"
               and s["quoteAsset"] == "USDT"
        ]
        print(f"✅ Found {len(symbols)} tradable USDT futures symbols.")
        return symbols
    except Exception as e:
        print("❌ Error fetching symbols:", e)
        return []

def backtest_symbols(symbols):
    all_summaries = []

    for sym in symbols:
        print(f"\n📡 Fetching {sym} ...")
        df = read_csv(sym)
        # df = fetch_klines_full_month(sym, INTERVAL, days=30)

        if df is None or df.empty:
            print(f"❌ No data for {sym}")
            continue

        trades, summary = backtest_strategy_improved(df)
        summary["symbol"] = sym
        all_summaries.append(summary)

        print_detailed_summary(summary, sym)

        # save trade file
        if not trades.empty:
            trades.to_csv(f"trades_{sym}.csv", index=False)
            print(f"💾 Saved trades_{sym}.csv")

    # summary df
    summary_df = pd.DataFrame(all_summaries)
    summary_df.to_csv("virtuum_backtest_results.csv", index=False)
    print("\n💾 Saved virtuum_backtest_results.csv")

    return summary_df
# ===== MAIN =====
# ===== MAIN =====
if __name__ == "__main__":
    # 🔍 Auto-fetch all tradable USDT perpetual futures
    symbols = ['1000BONKUSDT']

    print(f"\n🚀 Starting backtest for {len(symbols)} symbols...")
    results = backtest_symbols(symbols)

    print("\n📊 Summary Table:")
    print(results)

    # 💾 Save full results
    # results.to_csv("virtuum_backtest_results.csv", index=False)

    # 🔎 Filter profitable coins (net PnL > 0)
    profitable = results[results["total_pnl"] > 0].copy()
    profitable = profitable.sort_values(by="total_pnl", ascending=False)

    # 💾 Save only profitable coins
    profitable.to_csv("profitable_symbols.csv", index=False)

    print(f"\n💰 Found {len(profitable)} profitable coins!")
    print(profitable[["symbol", "total_pnl", "net_roi", "win_rate"]])
    print("💾 Saved profitable_symbols.csv")