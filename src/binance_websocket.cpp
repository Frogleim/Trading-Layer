#include "tools.hpp"
#include "binance_websocket.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <map>
#include <unordered_set>
#include "dashboard_logger.hpp"
#include "file_monitoring.hpp"
#include <fstream>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;


std::string env_path = ".env";
EnvData env = load_config(env_path);

// ====== Static configuration ======
const std::string MonitorTrades::API_KEY    = env.test_api_key;
const std::string MonitorTrades::API_SECRET = env.test_api_secret;
const std::string MonitorTrades::HOST       = env.test_base_url;
const std::string MonitorTrades::PORT       = "443";
const std::string MonitorTrades::TARGET     = "/ws-fapi/v1";  // private WebSocket endpoint

const double TP = 0.01;
const double SL = 0.005;

// ====== Global ASIO/Beast objects ======
namespace {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);
}

// ====== Utility ======
std::pair<double, double> calculate_sl_tp(std::string side, double entry_price) {
    double tp_price = 0.0, sl_price = 0.0;
    if (side == "LONG") {
        tp_price = entry_price * (1 + TP);
        sl_price = entry_price * (1 - SL);
    } else if (side == "SHORT") {
        tp_price = entry_price * (1 - TP);
        sl_price = entry_price * (1 + SL);
    }
    return {tp_price, sl_price};
}

// ====== Class Implementation ======
MonitorTrades::MonitorTrades()
    : ssl_ctx_(boost::asio::ssl::context::tlsv12_client),
      resolver_(io_ctx_),
      ws_(io_ctx_, ssl_ctx_),
      work_guard_(boost::asio::make_work_guard(io_ctx_)),
      connected_(false)
{
    ssl_ctx_.set_default_verify_paths();
}

MonitorTrades::~MonitorTrades() {
    work_guard_.reset();
    io_ctx_.stop();
}

// -------------------------
void MonitorTrades::connect() {
    try {
        auto const results = resolver_.resolve(HOST, PORT);
        net::connect(ws_.next_layer().next_layer(), results.begin(), results.end());

        if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), HOST.c_str()))
            throw boost::system::system_error(
                {static_cast<int>(::ERR_get_error()), boost::asio::error::get_ssl_category()},
                "Failed to set SNI hostname");

        ws_.next_layer().handshake(ssl::stream_base::client);
        ws_.handshake(HOST, TARGET);

        connected_ = true;
        std::cout << "✅ Connected to " << HOST << TARGET << std::endl;

    }
    catch (std::exception& e) {
        log_to_dashboard(e.what());
        std::cerr << "❌ Connect error: " << e.what() << std::endl;
        connected_ = false;

    }
}


void MonitorTrades::display_dashboard() {
    using namespace std::chrono_literals;

    while (true) {
        std::ostringstream out;
        {
            std::lock_guard<std::mutex> lock(mtx);

            out << "\033[H\033[J";  // clear screen
            out << "🧠 VIRTUUM HFT DASHBOARD (LIVE)\n";
            out << "Started: " << format_time(START_TIME) << "\n";
            out << "───────────────────────────────────────────────────────────────\n";
            out << std::left << std::setw(10) << "Symbol"
                << std::setw(10) << "Status"
                << std::setw(10) << "Dir"
                << std::setw(12) << "Entry"
                << std::setw(12) << "UnrealPnL"
                << std::setw(12) << "TotalPnL"
                << std::setw(8)  << "Trades"
                << std::setw(8)  << "Open" << "\n";
            out << "───────────────────────────────────────────────────────────────\n";

            double total_pnl = 0.0;
            for (auto& [sym, t] : active_positions) {
                double total_coin_pnl = t.realized_pnl + t.unrealized_pnl;
                total_pnl += total_coin_pnl;

                std::string pnl_color =
                    (total_coin_pnl > 0 ? "\033[32m" :
                     (total_coin_pnl < 0 ? "\033[31m" : "\033[0m"));

                out << std::left << std::setw(10) << sym
                    << std::setw(10) << (t.active ? "OPEN" : "IDLE")
                    << std::setw(10) << (t.direction.empty() ? "-" : t.direction)
                    << std::setw(12) << std::fixed << std::setprecision(4) << t.entry
                    << pnl_color << std::setw(12) << std::fixed << std::setprecision(2)
                    << t.unrealized_pnl << "\033[0m"
                    << pnl_color << std::setw(12) << std::fixed << std::setprecision(2)
                    << total_coin_pnl << "\033[0m"
                    << std::setw(8)  << t.total_trades
                    << std::setw(8)  << t.open_trades << "\n";
            }
            out << "───────────────────────────────────────────────────────────────\n";
            out << "🛰️  WSS Status:\n";
            out << "   Market Data: " << (g_market_wss_alive ? "✅" : "❌") << "\n";
            out << "   Trading/Account: " << (g_trading_wss_alive ? "✅" : "❌") << "\n";
            out << "───────────────────────────────────────────────────────────────\n";
            out << "💰 TOTAL PNL: "
                << (total_pnl >= 0 ? "\033[32m" : "\033[31m")
                << std::fixed << std::setprecision(2) << total_pnl << " USDT\033[0m\n";
            out << "───────────────────────────────────────────────────────────────\n";
            out << "📜 LOGS:\n";
        }

        // print logs
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            for (auto& line : recent_logs)
                out << line << "\n";
        }

        out << "───────────────────────────────────────────────────────────────\n";
        out << "Press Ctrl+C to exit.\n";

        {
            std::lock_guard<std::mutex> io_lock(io_mutex);
            std::cout << out.str() << std::flush;
        }

        std::this_thread::sleep_for(500ms);
    }
}

// -------------------------
void MonitorTrades::query_position() {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, cannot query position.\n";
        return;
    }

    long long ts = current_timestamp_ms();
    std::map<std::string, std::string> params = {
        {"apiKey", API_KEY},
        {"timestamp", std::to_string(ts)}
    };
    params["signature"] = generate_signature(params, API_SECRET);

    json req = {
        {"id", generate_uuid()},
        {"method", "v2/account.position"},
        {"params", params}
    };

    ws_.write(net::buffer(req.dump()));
}

// -------------------------
void MonitorTrades::market_order(const std::string& side,
                                 const std::string& symbol,
                                 double quantity,
                                 double entry_price) {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, skipping close order.\n";
        return;
    }

    if (symbol == "aiausdt") {
        quantity = 380;
    } else if (symbol == "coaiusdt") {
        quantity = 180;
    }

    long long ts = current_timestamp_ms();
    std::map<std::string, std::string> params = {
        {"apiKey", API_KEY},
        {"symbol", symbol},
        {"side", side},
        {"type", "MARKET"},
        {"positionSide", "BOTH"},
        {"quantity", std::to_string(quantity)},
        {"timestamp", std::to_string(ts)}
    };
    params["signature"] = generate_signature(params, API_SECRET);

    json req = {
        {"id", generate_uuid()},
        {"method", "order.place"},
        {"params", params}
    };

    try {
        ws_.write(net::buffer(req.dump()));
        // closing_trades_[symbol] = true;     // lock until confirmed
        // active_trades_.erase(symbol);

        std::cout << "📤 Sent close order: "
                  << side << " " << symbol
                  << " qty=" << quantity
                  << " entry=" << entry_price
                  << " (locked for confirmation)\n";
    }
    catch (std::exception& e) {
        std::cerr << "❌ Error sending close_trade: " << e.what() << std::endl;
    }
}

// -------------------------
void MonitorTrades::start_async_read() {
    ws_.async_read(
        buffer_,
        [this](beast::error_code ec, std::size_t bytes_transferred) {
            if (ec) {
                std::cerr << "❌ Read error: " << ec.message() << std::endl;
                log_to_dashboard("❌ Read error from start_async_read()");
                return;
            }

            std::string msg = beast::buffers_to_string(buffer_.data());
            buffer_.consume(buffer_.size());

            try {
                auto j = nlohmann::json::parse(msg);

                // 🧩 1️⃣ Handle order updates (status FILLED, CANCELED, etc.)
                if (j.contains("result") && j["result"].is_object()) {
                    const auto& r = j["result"];
                    if (r.contains("symbol") && r.contains("status")) {
                        std::string sym    = r.value("symbol", "");
                        std::string status = r.value("status", "");
                        if (status == "FILLED" || status == "PARTIALLY_FILLED" || status == "CANCELED") {
                            if (closing_trades_.erase(sym))
                                std::cout << "✅ Order fill confirmed, unlocking " << sym << std::endl;
                        }
                    }
                }

                // 🧩 2️⃣ Handle position snapshots
                if (j.contains("result") && j["result"].is_array()) {
                    std::unordered_set<std::string> snapshot_symbols;

                    for (const auto& pos : j["result"]) {
                        std::string symbol   = pos.value("symbol", "");
                        double entry         = std::stod(pos.value("entryPrice", "0"));
                        double posAmt        = std::stod(pos.value("positionAmt", "0"));
                        double markPrice     = std::stod(pos.value("markPrice", "0"));

                        if (symbol.empty()) continue;
                        snapshot_symbols.insert(symbol);

                        // --- A. Handle closed positions (amt = 0) ---
                        if (posAmt == 0) {
                            if (closing_trades_.count(symbol)) {
                                std::cout << "✅ " << symbol << " confirmed closed (positionAmt=0)\n";
                                closing_trades_.erase(symbol);
                            }
                            active_trades_.erase(symbol);
                            continue;
                        }

                        // --- B. Skip while waiting confirmation ---
                        if (closing_trades_.count(symbol)) {
                            std::cout << "⏳ Waiting for close confirmation: " << symbol << std::endl;
                            continue;
                        }

                        // --- C. Update active position ---
                        active_trades_[symbol] = posAmt;
                        std::string side = posAmt > 0 ? "LONG" : "SHORT";
                        auto [tp, sl] = calculate_sl_tp(side, entry);

                        // --- D. TP/SL logic ---
                        if (posAmt > 0) { // LONG
                            if (markPrice >= tp) {
                                std::cout << "🎯 TP Hit! Closing LONG for " << symbol << std::endl;
                                market_order("SELL", symbol, std::abs(posAmt), entry);
                                remove_symbol_from_file(symbol);

                                continue;
                            }
                            if (markPrice <= sl) {
                                std::cout << "🛑 SL Hit! Closing LONG for " << symbol << std::endl;
                                market_order("SELL", symbol, std::abs(posAmt), entry);
                                remove_symbol_from_file(symbol);

                                continue;
                            }
                        } else if (posAmt < 0) { // SHORT
                            if (markPrice <= tp) {
                                std::cout << "🎯 TP Hit! Closing SHORT for " << symbol << std::endl;
                                market_order("BUY", symbol, std::abs(posAmt), entry);
                                remove_symbol_from_file(symbol);

                                continue;
                            }
                            if (markPrice >= sl) {
                                std::cout << "🛑 SL Hit! Closing SHORT for " << symbol << std::endl;
                                market_order("BUY", symbol, std::abs(posAmt), entry);
                                remove_symbol_from_file(symbol);

                                continue;
                            }
                        }

                        std::cout << "📊 Position Update: "
                                  << symbol
                                  << " | entry=" << entry
                                  << " | amt=" << posAmt
                                  << " | mark=" << markPrice
                                    << " TP price: " << tp
                                    << " SL Price " << sl << std::endl;
                    }

                    // --- E. Confirm fully closed positions (not found in snapshot) ---
                    for (auto it = closing_trades_.begin(); it != closing_trades_.end(); ) {
                        if (!snapshot_symbols.count(it->first)) {
                            std::cout << "✅ " << it->first << " missing in snapshot — confirmed closed.\n";
                            it = closing_trades_.erase(it);
                        } else {
                            ++it;
                        }
                    }

                    // --- F. Display active trades summary ---
                    std::cout << "\n📈 Active Trades (" << active_trades_.size() << "):\n";
                    for (const auto& [sym, amt] : active_trades_)
                        std::cout << " • " << sym << " | amt=" << amt << std::endl;
                    std::cout << "────────────────────────────\n";
                }
                else if (!j.contains("result")) {
                    std::cout << "📩 Other message: " << msg << std::endl;
                }
            }
            catch (std::exception& e) {
                std::cerr << "❌ JSON parse error: " << e.what()
                          << "\nRaw msg: " << msg << std::endl;
            }

            start_async_read(); // continue reading
        });
}

// -------------------------
void MonitorTrades::run_event_loop() {
    io_ctx_.run();
}