# C++ Trading Layer

A high-performance, low-latency trading engine written in **C++** for real-time cryptocurrency futures trading.  
Designed for **micro-scalping and HFT-style strategies**, with a strong focus on execution speed, reliability, and precise trade lifecycle control.

---

## 🚀 Key Features

- **Real-time market data**
  - Binance Futures WebSocket streams
  - Mark price, positions, and account state monitoring

- **Low-latency order execution**
  - Asynchronous networking using **Boost.Asio / Boost.Beast**
  - Non-blocking REST + WebSocket handling

- **Signal ingestion**
  - External strategy signals via **ZeroMQ (ZMQ)**
  - Clean separation between strategy logic and execution layer

- **Advanced trade management**
  - Position tracking (LONG / SHORT)
  - Dynamic **Take Profit / Stop Loss**
  - Trade confirmation & reconciliation with exchange state

- **Risk & safety**
  - Automatic position cleanup on desync
  - Order/position consistency checks
  - Fail-safe trade closure logic

- **Observability**
  - Structured logging
  - Telegram notifications for trade events
  - Real-time status reporting

---

## 🧱 Architecture Overview


This document describes the high-level architecture of the **C++ Trading Layer**, its internal components, and how it integrates with external systems.

The system is designed to be **low-latency, fault-tolerant, and strategy-agnostic**, focusing exclusively on execution and trade lifecycle management.

---

## 🎯 Architectural Goals

- **Minimal latency** from signal → order
- **Strict separation** of strategy and execution
- **Deterministic trade lifecycle**
- **Exchange state as source of truth**
- **Fail-safe behavior on desynchronization**

---

---

## 🔄 Execution Flow

1. **Signal Generation**
   - Strategies generate trade intent externally (Python / ML / backtester).
   - Signals are sent via **ZeroMQ**.

2. **Signal Ingestion**
   - C++ Trading Layer receives signals asynchronously.
   - Validates direction, symbol, TP, SL, and position state.

3. **Order Placement**
   - Market order is sent via REST.
   - Entry price is confirmed using mark price.

4. **Position Synchronization**
   - WebSocket streams confirm open position.
   - Local state is reconciled with exchange state.

5. **Trade Management**
   - TP / SL levels are monitored in real time.
   - Positions are closed automatically on hit or invalid state.

6. **Exit & Cleanup**
   - Trade is finalized.
   - Local state is cleaned.
   - Notifications and confirmations are sent.

---

## 🧩 Core Modules

### Signal Gateway
- Receives external trade signals via ZMQ
- Ensures strategy independence
- Non-blocking ingestion

### Market Data Engine
- Maintains live mark prices
- Tracks account and position updates
- Handles reconnects and resubscriptions

### Order Execution Engine
- Sends market and conditional orders
- Uses async REST calls
- Designed for minimal blocking and retries

### Position & Risk Manager
- Tracks active trades per symbol
- Applies TP / SL logic
- Handles partial fills and desyncs

### Monitoring & Safety
- Detects exchange vs local mismatches
- Auto-closes positions on error
- Sends alerts via Telegram

---

## ⚙️ Concurrency Model

- **Boost.Asio io_context** for async networking
- Dedicated execution contexts for:
  - WebSockets
  - REST calls
  - ZMQ signal handling
- Minimal locking
- Symbol-level isolation where possible

---

## 🔐 State Management Philosophy

- Exchange state is authoritative
- Local state is continuously validated
- Any inconsistency triggers:
  - Trade reconciliation
  - Forced cleanup
  - Alerting

---

## 📈 Scalability Considerations

- Multi-symbol concurrent trading
- Strategy layer scales independently
- Execution layer optimized for CPU-bound latency
- Ready for future:
  - GPU offloading
  - Multi-exchange support
  - FPGA / low-level optimizations

---

## ⚠️ Failure Handling

| Scenario | Action |
|--------|--------|
| WebSocket disconnect | Auto-reconnect |
| Order confirmation missing | State reconciliation |
| Position desync | Forced close |
| Invalid signal | Reject & log |
| API error | Retry / alert |

---

## 🧠 Design Principles

- Execution is deterministic
- Strategy is external and replaceable
- Latency is a first-class concern
- Safety over silent failure

---

## 📄 Notes

This architecture is intentionally **lean and execution-focused**, making it suitable for high-frequency research, production trading, and rapid strategy iteration.

---

## 📌 Disclaimer

This system is for research and educational purposes.  
Trading leveraged instruments involves significant financial risk.




---

## 🛠 Tech Stack

- **Language:** C++17+
- **Networking:** Boost.Asio, Boost.Beast
- **Messaging:** ZeroMQ
- **JSON:** nlohmann/json
- **Exchange:** Binance Futures (USDT-M / COIN-M)
- **Notifications:** Telegram Bot API

---

## 📦 Core Components

- `MonitorTrades`
  - Tracks open positions
  - Validates exchange state vs local state
  - Handles TP / SL execution

- `BinanceWebSocket`
  - Market data & account updates
  - Position and order synchronization

- `Order Execution`
  - Market / conditional orders
  - Side, quantity, TP, SL handling

- `Signal Handler`
  - Receives external trade signals (ZMQ)
  - Converts strategy intent into executable orders

---

## ⚙️ Configuration

- Environment variables loaded from `.env`
- API keys, symbols, risk parameters configurable
- Supports multi-symbol concurrent trading

---

## 🔒 Design Philosophy

- **Execution ≠ Strategy**
- **Speed over abstraction**
- **Fail-safe > silent failure**
- **Exchange state is the source of truth**

This layer is intentionally kept **strategy-agnostic** and optimized purely for **execution correctness and latency**.

---

## 📈 Use Cases

- Micro-scalping strategies
- Automated futures trading
- HFT research & experimentation
- Python / ML strategy execution backend

---

## ⚠️ Disclaimer

This software is for **educational and research purposes**.  
Use at your own risk. Trading leveraged products involves significant risk.

---

