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
LEVERAGE = 50
MARGIN_PER_TRADE = 10.5
COOLDOWN_CANDLES = 1
LIMIT = 2000  # max klines per request
INTERVAL = "3m"  # timeframe


# ===== INDICATORS =====
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_obi(df):
    body = df["close"] - df["open"]
    rng = df["high"] - df["low"]
    ratio = np.where(rng == 0, 0, body / rng)
    return pd.Series(ratio).rolling(OBI_PERIOD).mean()


# ===== FETCH DATA =====
def fetch_klines_full_month(symbol: str, interval="5m", days=10):
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
def backtest_strategy_improved(df, initial_balance=893.0):
    df = df.copy()

    # Data validation
    df = df.drop_duplicates(subset=['open_time']).sort_values('open_time')
    df = df[df['volume'] > 0]

    if len(df) < EMA_PERIOD:
        return pd.DataFrame(), calculate_summary([], initial_balance, initial_balance)

    # Calculate indicators
    df["ema"] = compute_ema(df["close"], EMA_PERIOD)
    df["obi"] = compute_obi(df)

    # Generate signals
    df["signal"] = np.where(
        (df["obi"] > OBI_THRESHOLD) & (df["close"] > df["ema"]), "long",
        np.where((df["obi"] < -OBI_THRESHOLD) & (df["close"] < df["ema"]), "short", None)
    )

    trades = []
    cooldown = 0
    active_trade = None
    balance = initial_balance
    SLIPPAGE = 0.0001

    for i in range(len(df) - 1):
        if cooldown > 0:
            cooldown -= 1
            continue

        current_row = df.iloc[i]
        next_open = df["open"].iloc[i + 1]

        # Manage existing position
        if active_trade:
            current_high = df["high"].iloc[i]
            current_low = df["low"].iloc[i]

            if active_trade["direction"] == "long":
                hit_tp = current_high >= active_trade["tp"]
                hit_sl = current_low <= active_trade["sl"]
            else:
                hit_tp = current_low <= active_trade["tp"]
                hit_sl = current_high >= active_trade["sl"]

            if hit_tp or hit_sl:
                exit_price = active_trade["tp"] if hit_tp else active_trade["sl"]

                # Calculate PnL
                if active_trade["direction"] == "long":
                    pnl_pct = (exit_price - active_trade["entry"]) / active_trade["entry"]
                else:
                    pnl_pct = (active_trade["entry"] - exit_price) / active_trade["entry"]

                pnl_usdt = active_trade["position_size"] * pnl_pct * LEVERAGE
                pnl_usdt -= active_trade["position_size"] * FEE * 2

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

        # Open new position
        if (active_trade is None and current_row["signal"] in ("long", "short")
                and balance >= MARGIN_PER_TRADE):

            direction = current_row["signal"]
            position_size = min(MARGIN_PER_TRADE, balance * 0.1)

            if direction == "long":
                entry_price = next_open * (1 + SLIPPAGE)
                tp = entry_price * (1 + TP_PCT)
                sl = entry_price * (1 - SL_PCT)
            else:
                entry_price = next_open * (1 - SLIPPAGE)
                tp = entry_price * (1 - TP_PCT)
                sl = entry_price * (1 + SL_PCT)

            active_trade = {
                "direction": direction,
                "entry": entry_price,
                "tp": tp,
                "sl": sl,
                "entry_idx": i,
                "position_size": position_size
            }

    # Close any remaining position at the end
    if active_trade:
        final_close = df["close"].iloc[-1]
        if active_trade["direction"] == "long":
            pnl_pct = (final_close - active_trade["entry"]) / active_trade["entry"]
        else:
            pnl_pct = (active_trade["entry"] - final_close) / active_trade["entry"]

        pnl_usdt = active_trade["position_size"] * pnl_pct * LEVERAGE
        pnl_usdt -= active_trade["position_size"] * FEE * 2
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

    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    summary = calculate_summary(trades, initial_balance, balance)

    return trades_df, summary


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
# ===== MULTI-SYMBOL BACKTEST =====
def backtest_symbols(symbols):
    all_summaries = []
    for sym in symbols:
        print(f"📡 Fetching {sym} ...")
        df = fetch_klines_full_month(sym, INTERVAL, days=5)  # ✅ fixed param
        print(f"Fetched {len(df)} candles for {sym}")
        if df is None or df.empty:
            continue

        trades, summary = backtest_strategy_improved(df)
        summary["symbol"] = sym
        all_summaries.append(summary)
        print_detailed_summary(summary, sym)


        if not trades.empty:
            trades.to_csv(f"./trade_data/trades_{sym}.csv", index=False)
            print(f"💾 Saved trades_{sym}.csv")

        print(f"✅ {sym} done → {summary}")
        time.sleep(0.5)
    summary_df = pd.DataFrame(all_summaries)
    summary_df.to_csv("virtuum_backtest_results.csv", index=False)
    print("💾 Saved virtuum_backtest_results.csv")
    return summary_df



def calculate_summary(trades, initial_balance, final_balance):
    if not trades:
        return {
            "total_trades": 0,
            "win_rate": 0,
            "avg_pnl": 0,
            "total_pnl": 0,
            "initial_balance": round(initial_balance, 2),
            "final_balance": round(final_balance, 2),
            "net_roi": 0,
            "avg_win": 0,
            "avg_loss": 0,
            "profit_factor": 0,
            "max_drawdown": 0,
            "expectancy": 0,
            "sharpe_ratio": 0,
            "long_trades": 0,
            "short_trades": 0,
            "win_long": 0,
            "win_short": 0
        }

    trades_df = pd.DataFrame(trades)

    # Basic metrics
    total_trades = len(trades_df)
    winning_trades = trades_df[trades_df["pnl_usdt"] > 0]
    losing_trades = trades_df[trades_df["pnl_usdt"] < 0]
    win_rate = (len(winning_trades) / total_trades) * 100

    # PnL metrics
    total_pnl = trades_df["pnl_usdt"].sum()
    avg_pnl = trades_df["pnl_usdt"].mean()

    # Win/Loss metrics
    avg_win = winning_trades["pnl_usdt"].mean() if len(winning_trades) > 0 else 0
    avg_loss = losing_trades["pnl_usdt"].mean() if len(losing_trades) > 0 else 0

    # Profit Factor
    gross_profit = winning_trades["pnl_usdt"].sum() if len(winning_trades) > 0 else 0
    gross_loss = abs(losing_trades["pnl_usdt"].sum()) if len(losing_trades) > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else float('inf')

    # ROI
    net_roi = ((final_balance - initial_balance) / initial_balance) * 100

    # Drawdown calculation
    balances = [initial_balance]
    for trade in trades:
        balances.append(trade["balance_after"])

    running_max = pd.Series(balances).cummax()
    drawdowns = (pd.Series(balances) - running_max) / running_max * 100
    max_drawdown = drawdowns.min()

    # Expectancy
    expectancy = (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)

    # Sharpe Ratio (approximate)
    returns = trades_df["pnl_usdt"] / initial_balance
    if len(returns) > 1:
        sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(365 * 288)  # 5m periods in year
    else:
        sharpe_ratio = 0

    # Long/Short breakdown
    long_trades = trades_df[trades_df["direction"] == "long"]
    short_trades = trades_df[trades_df["direction"] == "short"]
    win_long = (long_trades["pnl_usdt"] > 0).mean() * 100 if len(long_trades) > 0 else 0
    win_short = (short_trades["pnl_usdt"] > 0).mean() * 100 if len(short_trades) > 0 else 0

    # Trade duration stats
    trades_df["duration"] = trades_df["exit_idx"] - trades_df["entry_idx"]
    avg_duration = trades_df["duration"].mean() if len(trades_df) > 0 else 0

    return {
        "total_trades": total_trades,
        "win_rate": round(win_rate, 2),
        "avg_pnl": round(avg_pnl, 4),
        "total_pnl": round(total_pnl, 2),
        "initial_balance": round(initial_balance, 2),
        "final_balance": round(final_balance, 2),
        "net_roi": round(net_roi, 2),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown": round(max_drawdown, 2),
        "expectancy": round(expectancy, 4),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "long_trades": len(long_trades),
        "short_trades": len(short_trades),
        "win_long": round(win_long, 2),
        "win_short": round(win_short, 2),
        "avg_duration_candles": round(avg_duration, 1),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(-gross_loss, 2)
    }


# ===== MAIN =====
if __name__ == "__main__":
    symbols = [ "AIAUSDT", "COAIUSDT", "VVVUSDT", "4USDT"]
    results = backtest_symbols(symbols)
    print("\n📊 Summary Table:")
    print(results)
    results.to_csv("virtuum_backtest_results.csv", index=False)