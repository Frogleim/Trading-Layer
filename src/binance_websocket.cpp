#include "tools.hpp"
#include "binance_websocket.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/ssl.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <map>
#include <unordered_set>
#include "file_monitoring.hpp"
#include <fstream>
#include <zmq.hpp>
#include "system_logger.hpp"

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;
using tcp = boost::asio::ip::tcp;
using ssl_stream = boost::asio::ssl::stream<tcp::socket>;
using websocket_t = boost::beast::websocket::stream<ssl_stream>;


std::string env_path = ".env";
EnvData env = load_config(env_path);



// ====== Static configuration ======
const std::string MonitorTrades::API_KEY    = env.is_testnet ? env.test_api_key    : env.api_key;
const std::string MonitorTrades::API_SECRET = env.is_testnet ? env.test_api_secret : env.api_secret;
const std::string MonitorTrades::HOST       = env.is_testnet ? env.test_base_url   : env.base_url;
const std::string MonitorTrades::MARK_PRICE_HOST = env.market_data;

const std::string MonitorTrades::PORT   = "443";
const std::string MonitorTrades::TARGET = "/ws-fapi/v1";  // private WebSocket endpoint

const double TP = env.TP;
const double SL = env.SL;
constexpr double SL_BUFFER = 0.001;
// ====== Global ASIO/Beast objects ======
namespace {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);
}

// ====== Utility ======
std::pair<double, double> calculate_sl_tp(const std::string& side, double entry_price, double mark_price) {
    double tp_price = 0.0, sl_price = 0.0;

    constexpr double SL_BUFFER = 0.001; // 0.1% safety margin

    if (side == "LONG") {
        tp_price = entry_price * (1 + TP);
        // use mark drift compensation: if mark is already below entry, shift SL slightly lower
        double drift = (entry_price - mark_price) / entry_price;
        sl_price = entry_price * (1 - SL - drift);
        sl_price *= (1.0 + SL_BUFFER);
    } else if (side == "SHORT") {
        tp_price = entry_price * (1 - TP);
        double drift = (mark_price - entry_price) / entry_price;
        sl_price = entry_price * (1 + SL + drift);
        sl_price *= (1.0 - SL_BUFFER);
    }

    return {tp_price, sl_price};
}

// ====== Class Implementation ======
MonitorTrades::MonitorTrades()
    : ssl_ctx_(ssl::context::tlsv12_client),
      resolver_private_(io_private_),
      resolver_mark_(io_mark_),
      work_guard_private_(net::make_work_guard(io_private_)),
      work_guard_mark_(net::make_work_guard(io_mark_)),
      connected_(false),
        zmq_pub_(zmq_ctx_, zmq::socket_type::pub)
{
    ssl_ctx_.set_default_verify_paths();
    zmq_pub_.bind("tcp://localhost:5556");
    std::cout << "📡 ZMQ publisher bound on tcp://localhost:5556\n";
    // Start independent IO threads
    thread_private_ = std::thread([this](){ io_private_.run(); });
    thread_mark_    = std::thread([this](){ io_mark_.run(); });
}

MonitorTrades::~MonitorTrades(){
    work_guard_private_.reset();
    work_guard_mark_.reset();
    io_private_.stop();
    io_mark_.stop();

    if(thread_private_.joinable()) thread_private_.join();
    if(thread_mark_.joinable())    thread_mark_.join();
}

// -------------------------
void MonitorTrades::connect(){
    try {
        // === PRIVATE WS ===
        ws_ = std::make_unique<websocket_t>(io_private_, ssl_ctx_);
        auto results = resolver_private_.resolve(HOST, PORT);
        net::connect(ws_->next_layer().next_layer(), results.begin(), results.end());

        if(!SSL_set_tlsext_host_name(ws_->next_layer().native_handle(), HOST.c_str()))
            throw boost::system::system_error(
                {static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()},
                "Failed SNI hostname");

        ws_->next_layer().handshake(ssl::stream_base::client);
        ws_->handshake(HOST, TARGET);
        connected_ = true;
        std::cout<<"✅ Connected to "<<HOST<<TARGET<<std::endl;
        start_async_read();

        // === MARKPRICE WS ===
        std::vector<std::string> symbols = {
            "1000satsusdt",
            "jellyjellyusdt",
                    "gtcusdt",
                    "coaiusdt",
                    "labusdt",
                    "arusdt",
                    "evaausdt",
                    "pippinusdt"
            };

        std::string combined="/stream?streams=";
        for(size_t i=0;i<symbols.size();++i){
            combined+=symbols[i]+"@markPrice@1s";
            if(i+1<symbols.size()) combined+="/";
        }

        ws_mark_ = std::make_unique<websocket_t>(io_mark_, ssl_ctx_);
        auto mark_results = resolver_mark_.resolve(MARK_PRICE_HOST, PORT);
        net::connect(ws_mark_->next_layer().next_layer(), mark_results.begin(), mark_results.end());

        if(!SSL_set_tlsext_host_name(ws_mark_->next_layer().native_handle(), MARK_PRICE_HOST.c_str()))
            throw boost::system::system_error(
                {static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()},
                "Failed SNI hostname");

        ws_mark_->next_layer().handshake(ssl::stream_base::client);
        ws_mark_->handshake(MARK_PRICE_HOST, combined);
        std::cout<<"✅ Subscribed to combined markPrice stream:\n"
                 <<MARK_PRICE_HOST<<combined<<std::endl;

        start_markprice_read();

    }catch(const std::exception& e){
        std::cerr<<"❌ Connect error: "<<e.what()<<std::endl;
        connected_=false;
    }
}

void MonitorTrades::start_markprice_read() {
    auto mark_buffer = std::make_shared<beast::flat_buffer>();

    ws_mark_->async_read(*mark_buffer,
        [this, mark_buffer](beast::error_code ec, std::size_t bytes_transferred) {
            if (ec) {
                std::cerr << "❌ MarkPrice read error: " << ec.message() << std::endl;
                return;
            }

            std::string msg = beast::buffers_to_string(mark_buffer->data());
            mark_buffer->consume(mark_buffer->size());

            try {
                auto j = nlohmann::json::parse(msg);

                // ✅ Combined stream structure: { "stream": "...", "data": {...} }
                if (j.contains("stream") && j.contains("data")) {
                    const auto& d = j["data"];
                    std::string symbol = d.value("s", "");
                    double mark_price  = std::stod(d.value("p", "0"));

                    if (symbol.empty()) return;

                    // 🧩 If we have an active trade for this symbol
                    if (active_trades_.count(symbol)) {
                        auto& trade = active_trades_.at(symbol);

                        // Skip if already closing
                        if (closing_trades_.count(symbol))
                            return;

                        bool hit_tp = false;
                        bool hit_sl = false;

                        if (trade.side == "LONG") {
                            hit_tp = mark_price >= trade.tp;
                            hit_sl = mark_price <= trade.sl;
                        } else if (trade.side == "SHORT") {
                            hit_tp = mark_price <= trade.tp;
                            hit_sl = mark_price >= trade.sl;
                        }

                        if (hit_tp || hit_sl) {
                            std::string reason = hit_tp ? "🎯 TP hit" : "🛑 SL hit";
                            std::cout << reason << " for " << symbol
                                      << " mark=" << mark_price
                                      << " entry=" << trade.entry
                                      << " tp=" << trade.tp
                                      << " sl=" << trade.sl << std::endl;

                            std::ostringstream oss;
                            oss << reason << "for " << symbol << " mark=" << mark_price << " entry="
                            << trade.entry << " tp=" << trade.tp << " sl=" << trade.sl << std::endl;
                            Logger::info(oss.str());

                            closing_trades_[symbol] = true;

                            // ✅ Execute close order safely inside the private io_context
                            net::post(io_private_, [this, symbol, trade]() {
                                std::string close_side =
                                    (trade.side == "LONG") ? "SELL" : "BUY";
                                market_order(close_side, symbol, trade.amount, trade.entry);
                            });
                            active_trades_.erase(symbol);
                            send_confirmation(symbol);

                        }
                    }
                }

            } catch (const std::exception& e) {
                std::cerr << "❌ MarkPrice parse error: " << e.what() << std::endl;
            }

            // Continue reading recursively
            start_markprice_read();
        });
}


void MonitorTrades::start_zmq_listener(){
    zmq_thread_=std::thread([this](){
        try{
            zmq::socket_t subscriber(zmq_ctx_,zmq::socket_type::sub);
            const std::string endpoint="tcp://localhost:5555";
            subscriber.connect(endpoint);
            subscriber.set(zmq::sockopt::subscribe,"");
            std::cout<<"📡 Listening for signals on "<<endpoint<<std::endl;

            while(true){
                zmq::message_t msg;
                if(!subscriber.recv(msg,zmq::recv_flags::none)) continue;
                auto t_recv=std::chrono::high_resolution_clock::now();
                std::string data(static_cast<char*>(msg.data()),msg.size());
                std::cout<<"📨 Received signal: "<<data<<std::endl;

                std::istringstream iss(data);
                std::string symbol,direction;
                iss>>symbol>>direction;
                if(symbol.empty()||direction.empty()) continue;

                double amount=5.0;
                double price=0.0;
                std::string side=(direction=="LONG")?"BUY":"SELL";

                net::post(io_private_,[this,side,symbol,amount,price,t_recv](){
                    auto t_exec=std::chrono::high_resolution_clock::now();
                    auto latency_us=
                        std::chrono::duration_cast<std::chrono::microseconds>(t_exec-t_recv).count();
                    std::cout<<"⏱️ Signal-to-order latency for "<<symbol<<" = "
                             <<latency_us<<" µs ("<<latency_us/1000.0<<" ms)\n";
                    market_order(side,symbol,amount,price);
                });
            }
        }catch(const std::exception& e){
            std::cerr<<"❌ ZMQ listener error: "<<e.what()<<std::endl;
        }
    });
    zmq_thread_.detach();
}


void MonitorTrades::send_confirmation(std::string symbol) {
    try {
        std::lock_guard<std::mutex> lock(zmq_mutex_);
        std::string message = "CLOSED " + symbol;
        zmq_pub_.send(zmq::buffer(message), zmq::send_flags::none);
        std::cout << "✅ Sent trade confirmation: " << message << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "❌ Failed to send ZMQ message: " << e.what() << std::endl;
    }
}


// -------------------------
void MonitorTrades::query_position() {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, cannot query position.\n";
        return;
    }

    net::post(io_private_, [this]() {
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

        try {
            ws_->write(net::buffer(req.dump()));
            std::cout << "📤 Requested account positions...\n";
        } catch (const std::exception& e) {
            std::cerr << "❌ query_position error: " << e.what() << std::endl;
        }
    });
}

// -------------------------
void MonitorTrades::market_order(const std::string& side,
                                 const std::string& symbol,
                                 double quantity,
                                 double entry_price)
{
    if (!connected_) {
        std::cerr << "⚠️ Not connected, skipping close order.\n";
        return;
    }

    // === replace the if/else chain ===
    static const std::unordered_map<std::string, double> quantity_map = {
        // {"1000satsusdt", 35000000},
        // {"jellyjellyusdt", 50},
        // {"gtcusdt", 1200},
        // {"flmusdt", 10000},
        // {"labusdt", 250},
        {"coaiusdt", 120},
        // {"evaausdt", 300},
        // {"pippinusdt", 300},


    };

    // check if symbol is known and override
    if (auto it = quantity_map.find(symbol); it != quantity_map.end()) {
        quantity = it->second;
    }

    if (quantity <= 0) {
        std::cerr << "⚠️ Skipping market_order for " << symbol
                  << " because quantity=" << quantity << " (<=0)\n";
        return;
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
        ws_->write(net::buffer(req.dump()));
        std::cout << "📤 Sent close order: "
                  << side << " " << symbol
                  << " qty=" << quantity
                  << " entry=" << entry_price
                  << " (locked for confirmation)\n";
    } catch (std::exception& e) {
        std::cerr << "❌ Error sending close_trade: " << e.what() << std::endl;
    }
}

// -------------------------
void MonitorTrades::start_async_read() {
    ws_->async_read(
        buffer_,
        [this](beast::error_code ec, std::size_t bytes_transferred) {
            if (ec) {
                std::cerr << "❌ Read error: " << ec.message() << std::endl;

                // Common non-fatal disconnects
                if (ec == websocket::error::closed || ec == beast::websocket::error::closed
                    || ec.message() == "End of file") {

                    std::cerr << "🔁 Attempting to reconnect to Binance...\n";
                    try {
                        ws_->next_layer().shutdown();
                    } catch (...) {}

                    connected_ = false;
                    connect();   // reconnect logic
                }
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
                            std::cout << "✅ " << symbol << " confirmed closed (positionAmt=0)\n";
                            closing_trades_.erase(symbol);
                            active_trades_.erase(symbol);
                            continue;
                        }

                        // --- B. Skip while waiting confirmation ---
                        if (closing_trades_.count(symbol)) {
                            std::cout << "⏳ Waiting for close confirmation: " << symbol << std::endl;
                            continue;
                        }
                        std::cout << "\n📈 Active Trades (" << active_trades_.size() << "):\n";

                        // --- C. Update active position ---
                        ActiveTrade t;
                        t.side   = posAmt > 0 ? "LONG" : "SHORT";
                        t.entry  = entry;
                        t.amount = std::abs(posAmt);
                        auto [tp, sl] = calculate_sl_tp(t.side, entry, markPrice);
                        t.tp = tp;
                        t.sl = sl;
                        active_trades_[symbol] = t;


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
                    // --- F. Display active trades summary ---
                    static size_t last_lines = 0;
                    {
                        std::lock_guard<std::mutex> lock(log_mutex);

                        std::ostringstream oss;
                        oss << "────────────────────────────\n";
                        oss << "📈 Active Trades (" << active_trades_.size() << "):\n";

                        if (active_trades_.empty()) {
                            oss << " • none\n";
                        } else {
                            for (const auto& [sym, trade] : active_trades_) {
                                oss << " • " << sym
                                    << " | side="  << trade.side
                                    << " | amt="   << trade.amount
                                    << " | entry=" << trade.entry
                                    << " | tp="    << trade.tp
                                    << " | sl="    << trade.sl
                                    << "\n";
                            }
                        }

                        oss << "────────────────────────────"; // ⚠️ no trailing \n here

                        std::string block = oss.str();
                        size_t current_lines = std::count(block.begin(), block.end(), '\n') + 1;

                        // Move cursor up to overwrite previous dashboard fully
                        if (last_lines > 0)
                            std::cout << "\033[" << last_lines << "F"; // Move cursor up N lines to start of block

                        // Now clear each line before printing the new block
                        std::istringstream iss(block);
                        std::string line;
                        while (std::getline(iss, line)) {
                            std::cout << "\033[2K" << line << "\n"; // 2K clears entire line
                        }

                        std::cout << std::flush;
                        last_lines = current_lines;
                    }

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
    // just block main thread until background I/O threads finish
    if (thread_private_.joinable()) thread_private_.join();
    if (thread_mark_.joinable())    thread_mark_.join();
}