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
#include <deque>
#include "system_logger.hpp"
#include "logger.hpp"
#include <numeric>
#include <cmath>
#include "telegram.hpp"

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

static size_t last_active_count = SIZE_MAX;
static bool last_had_trades = false;

// ====== Static configuration ======
const std::string MonitorTrades::API_KEY    = env.is_testnet ? env.test_api_key    : env.api_key;
const std::string MonitorTrades::API_SECRET = env.is_testnet ? env.test_api_secret : env.api_secret;
const std::string MonitorTrades::HOST       = env.is_testnet ? env.test_base_url   : env.base_url;
const std::string MonitorTrades::MARK_PRICE_HOST = env.market_data;
double pos_amt = env.pos_amt;

const std::string MonitorTrades::PORT   = "443";
const std::string MonitorTrades::TARGET = "/ws-fapi/v1";  // private WebSocket endpoint

const double TP = env.TP;
const double SL = env.SL;
constexpr double SL_BUFFER = 0.001;



void MonitorTrades::render_dashboard() {
    bool has_trades = !active_trades_.empty();

    // 🔒 Do not redraw if nothing changed
    if (active_trades_.size() == last_active_count &&
        has_trades == last_had_trades)
        return;

    last_active_count = active_trades_.size();
    last_had_trades   = has_trades;

    static size_t last_lines = 0;

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

    oss << "────────────────────────────";

    std::string block = oss.str();
    size_t current_lines = std::count(block.begin(), block.end(), '\n') + 1;

    if (last_lines > 0)
        std::cout << "\033[" << last_lines << "F";

    std::istringstream iss(block);
    std::string line;
    while (std::getline(iss, line)) {
        std::cout << "\033[2K" << line << "\n";
    }

    std::cout << std::flush;
    last_lines = current_lines;
}

std::string format_price(double price, const std::string& symbol) {
    // Find tick size for this symbol
    double tick = 0.0001;  // default
    if (auto it = TICK_SIZE_MAP.find(symbol); it != TICK_SIZE_MAP.end())
        tick = it->second;

    // Round price to nearest tick
    double rounded = std::round(price / tick) * tick;

    // Determine decimal places based on tick size
    int decimals = 0;
    double tmp = tick;
    while (tmp < 1.0) {
        tmp *= 10.0;
        decimals++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(decimals) << rounded;
    return oss.str();
}

std::unordered_map<std::string, OrderBookInfo> orderbook_;
std::unordered_map<std::string, OrderBookLogs> order_book_logger_;


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
            "celousdt"
            };

        std::string combined="/stream?streams=";
        for(size_t i=0;i<symbols.size();++i){
            combined+=symbols[i]+"@markPrice@1s/" + symbols[i] + "@depth10@100ms";
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
                start_markprice_read();
                return;
            }

            std::string msg = beast::buffers_to_string(mark_buffer->data());
            mark_buffer->consume(mark_buffer->size());

            try {
                auto j = nlohmann::json::parse(msg);

                if (j.contains("stream") && j.contains("data")) {
                    std::string stream = j.value("stream", "");
                    const auto& d = j["data"];
                    std::string symbol = d.value("s", "");
                    std::transform(symbol.begin(), symbol.end(), symbol.begin(), ::tolower);
                   if (stream.find("@markPrice") != std::string::npos) {
                        double mark_price = std::stod(d.value("p", "0"));
                        if (mark_price <= 0.0) {
                            start_markprice_read();
                            return;
                        }

                        latest_mark_prices_[symbol] = mark_price;
                        if (active_trades_.count(symbol)) {
                                auto& trade = active_trades_.at(symbol);

                                if (closing_trades_.count(symbol))
                                    goto next; // skip if already closing

                                bool hit_tp = false;
                                bool hit_sl = false;

                                if (trade.side == "LONG" || trade.side == "BUY") {
                                    hit_tp = mark_price >= trade.tp;
                                    hit_sl = mark_price <= trade.sl;
                                } else {
                                    hit_tp = mark_price <= trade.tp;
                                    hit_sl = mark_price >= trade.sl;
                                }

                                if (hit_tp) {
                                    close_trade(symbol, trade, "TP");
                                }
                                else if (hit_sl) {
                                    close_trade(symbol, trade, "SL");
                                }
                            }
                            next:;
                    }
                }
            }
            catch (const std::exception& e) {
                std::cerr << "❌ Parse error: " << e.what()
                          << "\nRaw: " << msg << std::endl;
            }


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
                Telegram telegram;
                zmq::message_t msg;
                if(!subscriber.recv(msg,zmq::recv_flags::none)) continue;
                auto t_recv=std::chrono::high_resolution_clock::now();
                std::string data(static_cast<char*>(msg.data()),msg.size());
                std::cout<<"📨 Received signal: "<<data<<std::endl;
                std::thread telegram_send([=]() mutable {
                   telegram.send_msg("📨 Received signal: " + data);
                });
                telegram_send.detach();
                // Fallback: plain text "symbol side TP= SL="
                std::string symbol, direction, tps, sls;
                std::istringstream iss(data);
                iss >> symbol >> direction >> tps >> sls;

                double tp = 0.0, sl = 0.0;

                if(tps.rfind("TP=",0)==0) tp = std::stod(tps.substr(3));
                if(sls.rfind("SL=",0)==0) sl = std::stod(sls.substr(3));


                if(symbol.empty()||direction.empty()) continue;



                net::post(io_private_,[this,direction,symbol,t_recv, tp, sl](){
                    auto t_exec=std::chrono::high_resolution_clock::now();
                    auto latency_us=
                        std::chrono::duration_cast<std::chrono::microseconds>(t_exec-t_recv).count();
                    std::cout<<"⏱️ Signal-to-order latency for "<<symbol<<" = "
                             <<latency_us<<" µs ("<<latency_us/1000.0<<" ms)\n";

                    handle_external_signal(symbol,direction,tp,sl);
                });



            }
        }catch(const std::exception& e){
            std::cerr<<"❌ ZMQ listener error: "<<e.what()<<std::endl;
        }
    });
    zmq_thread_.detach();
}


void MonitorTrades::send_confirmation(const std::string& symbol) {
    try {
        std::lock_guard<std::mutex> lock(zmq_mutex_);
        std::string message = "CLOSED " + symbol;
        zmq_pub_.send(zmq::buffer(message), zmq::send_flags::none);
        logger.info("✅ Sent trade confirmation: " + message);
        std::cout << "✅ Sent trade confirmation: " << message << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "❌ Failed to send ZMQ message: " << e.what() << std::endl;
    }
}


// -------------------------
void MonitorTrades::query_position() {
    if (!connected_ || !ws_) {
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
        } catch (const std::exception& e) {
            std::cerr << "❌ query_position error: " << e.what() << std::endl;
        }
    });
}

void MonitorTrades::close_trade(
    const std::string& symbol,
    const ActiveTrade& trade,
    const std::string& reason
) {
    if (closing_trades_.count(symbol))
        return; // already closing
    closing_trades_.insert({symbol, true});
    std::string close_side =
        (trade.side == "LONG" || trade.side == "BUY") ? "SELL" : "BUY";

    std::cout << "🚨 Closing " << symbol
              << " reason=" << reason
              << " mark hit\n";

    Telegram telegram;
    telegram.send_msg(
        "🚨 " + symbol +
        " " + reason +
        " hit | entry=" + std::to_string(trade.entry) +
        " tp=" + std::to_string(trade.tp) +
        " sl=" + std::to_string(trade.sl)
    );

    market_order(close_side, symbol, trade.amount, latest_mark_prices_[symbol]);
    send_confirmation(symbol);
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
      {"celousdt", pos_amt}


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


void MonitorTrades::handle_external_signal(const std::string& symbol,
                                           const std::string& side,
                                           double tp, double sl)
{
    double amount = pos_amt;

    std::cout << "🎯 External Signal Received:\n"
              << " symbol=" << symbol
              << " side="   << side
              << " TP="     << tp
              << " SL="     << sl
              << std::endl;


    std::string close_side;
    std::string symbol_lower = to_lower_symbol(symbol);
    double mark = latest_mark_prices_[symbol];


    ActiveTrade t;
    t.side   = side;
    t.entry  = mark;
    t.tp     = tp;
    t.sl     = sl;
    t.amount = amount;

    active_trades_[symbol] = t;

    // Send entry order
    net::post(io_private_, [this, side, symbol, amount, mark]() {
        market_order(side, symbol, amount, mark);

        std::this_thread::sleep_for(std::chrono::milliseconds(500));

    });
    if (side == "BUY")
        close_side = "SELL";
    else
        close_side = "BUY";




}


void MonitorTrades::algo_SL_orders(const std::string &side, const std::string &symbol,
    double &quantity,  double &sl) {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, skipping close order.\n";
        return;
    }


    Logger::info("Algo SL order params: \nSymbol: "
        + symbol + + " SL: " + std::to_string(sl)
        + "\nSide: " + side + "\nQuantity: " +
        std::to_string(quantity));


    std::string upper_symbol = to_upper_symbol(symbol);

    long long ts = current_timestamp_ms();
    std::map<std::string, std::string> params = {
        {"algoType", "CONDITIONAL"},
        {"apiKey", API_KEY},
        {"symbol", upper_symbol},
        {"side", side},
        {"positionSide", "BOTH"},
        {"closePosition", "true"},
        {"type", "STOP_MARKET"},
        {"triggerPrice", std::to_string(sl)},
        {"timestamp", std::to_string(ts)},
    };
    params["signature"] = generate_signature(params, API_SECRET);
    json req = {
        {"id", generate_uuid()},
        {"method", "algoOrder.place"},
        {"params", params}
    };
    try {
        ws_->write(net::buffer(req.dump()));

        std::cout << "📤 Sent Algo order: "
                  << side << " " << symbol
                  << " qty=" << quantity
                  << " (locked for confirmation)\n";

    } catch (std::exception& e) {
        std::cerr << "❌ Error sending close_trade: " << e.what() << std::endl;
    }

}



void MonitorTrades::algo_TP_orders(const std::string &side, const std::string &symbol,
    double &quantity,
double &tp) {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, skipping close order.\n";
        return;
    }

    logger.info("Algo order params: \nSymbol: " + symbol + + " TP: " + std::to_string(tp) +
        "\nSide: " + side + "\nQuantity: " +
        std::to_string(quantity)) ;

    std::string upper_symbol = to_upper_symbol(symbol);

    long long ts = current_timestamp_ms();
    std::map<std::string, std::string> params = {
        {"algoType", "CONDITIONAL"},
        {"apiKey", API_KEY},
        {"symbol", upper_symbol},
        {"side", side},
        {"positionSide", "BOTH"},
        {"closePosition", "true"},
        {"type", "TAKE_PROFIT_MARKET"},
        {"triggerPrice", std::to_string(tp)},   // ← REQUIRED!!!
        {"timestamp", std::to_string(ts)},

    };
    params["signature"] = generate_signature(params, API_SECRET);
    json req = {
        {"id", generate_uuid()},
        {"method", "algoOrder.place"},
        {"params", params}
    };
    try {
        ws_->write(net::buffer(req.dump()));
        std::cout << "📤 Sent Algo order: "
                  << side << " " << symbol
                  << " qty=" << quantity
                  << " (locked for confirmation)\n";
    } catch (std::exception& e) {
        std::cerr << "❌ Error sending close_trade: " << e.what() << std::endl;
    }

}


void MonitorTrades::check_positions_exit(const json& query_data) {
    Telegram telegram;
    if (!query_data.is_object() || !query_data.contains("result")) return;

    const auto& result = query_data["result"];

    // This function only handles position snapshots
    if (!result.is_array()) return;

    bool empty_result = result.empty();
    for (const auto& [symbol, qty] : this->SYMBOLS) {

        bool has_active = active_trades_.count(symbol) > 0;


        // ----------------------------
        // CASE 1: Binance shows NO position → remove stale local state
        // ----------------------------
            if (empty_result && has_active) {
                Logger::info("❌ Binance shows no position. Removing local trade for " + symbol);
                active_trades_.erase(symbol);
                algo_orders_created_.erase(symbol);
                send_confirmation(symbol);
                telegram.send_msg("Position disappeared from Binance: " + symbol);
                continue;
        }


        // ----------------------------
        // CASE 2: Binance shows position but local state missing
        // ----------------------------
        if (!empty_result && !has_active) {
            for (const auto& pos : query_data["result"]) {

                std::string sym = to_lower_symbol(pos.value("symbol", ""));
                if (sym != symbol) continue;  // only handle matching symbol

                double posAmt     = std::stod(pos.value("positionAmt", "0"));
                double markPrice  = std::stod(pos.value("markPrice", "0"));
                double pnl        = std::stod(pos.value("unrealizedPnl", "0"));
                std::string side  = (posAmt > 0) ? "LONG" : "SHORT";

                // Use quantity FROM GLOBAL MAP
                double qty = SYMBOLS.at(sym);
                algo_orders_created_.erase(sym);

                market_order(side, sym, qty, markPrice);
                send_confirmation(sym);
                telegram.send_msg(
                    "Recovered missing local state for " + sym +
                    " | pnl=" + std::to_string(pnl)
                );
            }
        }
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
                Telegram telegram;
                auto j = nlohmann::json::parse(msg);
                // 🔥 LOG ALL RESPONSES
                if (j.contains("id")) {
                    std::cout << "📥 Binance RESPONSE:\n"
                              << j.dump(2) << std::endl;
                }
                net::post(io_private_, [this, j]() {
                        check_positions_exit(j);
                });






                // 🧩 1️⃣ Handle order updates (status FILLED, CANCELED, etc.)
                if (j.contains("result") && j["result"].is_object()) {
                    const auto& r = j["result"];
                    if (r.contains("symbol") && r.contains("status")) {
                        std::string sym    = r.value("symbol", "");
                        std::string status = r.value("status", "");
                        if (status == "FILLED" || status == "NEW" || status == "CANCELED") {
                            logger.info("Position: " + status + " | " + sym + " | " + status);
                            if (closing_trades_.erase(sym))
                                std::cout << "✅ Order fill confirmed, unlocking " << sym << std::endl;
                        }
                    }
                }


                if (j.contains("result") && j["result"].is_array()) {
                    std::unordered_set<std::string> snapshot_symbols;
                    for (const auto& pos : j["result"]) {
                        std::string symbol   = to_lower_symbol(pos.value("symbol", ""));
                        double posAmt        = std::stod(pos.value("positionAmt", "0"));
                        if (symbol.empty()) continue;
                        snapshot_symbols.insert(symbol);
                        double tp_price = active_trades_[symbol].tp;
                        double sl_price = active_trades_[symbol].sl;
                        std::string closing_side;
                        std::string close_side = (posAmt > 0) ? "SELL" : "BUY";
                        algo_SL_orders(close_side, symbol, pos_amt, sl_price);
                        algo_TP_orders(close_side, symbol, pos_amt, tp_price);

                        if (posAmt == 0) {
                            std::cout << "✅ " << symbol << " confirmed closed (positionAmt=0)\n";
                            closing_trades_.erase(symbol);
                            active_trades_.erase(symbol);
                            continue;
                        }
                        if (closing_trades_.count(symbol)) {
                            std::cout << "⏳ Waiting for close confirmation: " << symbol << std::endl;
                            continue;
                        }
                        std::cout << "\n📈 Active Trades (" << active_trades_.size() << "):\n";
                        bool state_changed = false;

                        if (posAmt == 0) {
                            state_changed = true;
                        }
                        if (!active_trades_.empty()) {
                            state_changed = true;
                        }
                        if (state_changed)
                            render_dashboard();


                    }

                    for (auto it = closing_trades_.begin(); it != closing_trades_.end(); ) {
                        if (!snapshot_symbols.count(it->first)) {
                            std::cout << "✅ " << it->first << " missing in snapshot — confirmed closed.\n";
                            it = closing_trades_.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }

            }
            catch (std::exception& e) {
                std::cerr << "❌ JSON parse error: " << e.what()
                          << "\nRaw msg: " << msg << std::endl;
            }

            start_async_read(); // continue reading
        });
}

void MonitorTrades::run_event_loop() {
    // just block main thread until background I/O threads finish
    if (thread_private_.joinable()) thread_private_.join();
    if (thread_mark_.joinable())    thread_mark_.join();
}
