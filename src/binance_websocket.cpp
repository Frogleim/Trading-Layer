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
#include <zmq.hpp>
#include "system_logger.hpp"
#include "tools.hpp"
#include "logger.hpp"

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;
using tcp = boost::asio::ip::tcp;
using ssl_stream = boost::asio::ssl::stream<tcp::socket>;
using websocket_t = boost::beast::websocket::stream<ssl_stream>;

#include <iomanip>
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
std::pair<double, double> calculate_sl_tp(const std::string& side, double entry_price, std::string symbol) {
    double tp_price = 0.0, sl_price = 0.0;

    constexpr double SL_BUFFER = 0.001; // 0.1% safety margin
    if (symbol == "jellyjellyusdt") {
        if (side == "LONG") {
            tp_price = entry_price * (1 + 0.01);
            sl_price = entry_price * (1 - 0.005);
        } else if (side == "SHORT") {
            tp_price = entry_price * (1 - 0.01);
            sl_price = entry_price * (1 + 0.005);
        }

    } else {
        if (side == "LONG") {
            tp_price = entry_price * (1 + TP);
            sl_price = entry_price * (1 - SL);

        } else if (side == "SHORT") {
            tp_price = entry_price * (1 - TP);
            sl_price = entry_price * (1 + SL);
        }
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
    std::string is_demo = env.is_testnet ? "True" : "False";
    tg_worker.push("📡 ZMQ publisher bound on tcp://localhost:5556\n");
    tg_worker.push("Is Demo account: " + is_demo);
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
            combined+=symbols[i]+"@depth10@100ms";
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
        std::string msg =
            "✅ Subscribed to combined markPrice stream:\n" +
            MARK_PRICE_HOST + combined;

        tg_worker.push(msg);

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
                _mark_price_logger.str("");
                _mark_price_logger.clear();
                _mark_price_logger << "❌ MarkPrice read error: " + ec.message();
                std::cerr << "❌ MarkPrice read error: " << ec.message() << std::endl;
                Logger::error(_mark_price_logger.str());
                return;
            }

            std::string msg = beast::buffers_to_string(mark_buffer->data());
            mark_buffer->consume(mark_buffer->size());

            try {

               auto j = json::parse(msg);
                if (!j.contains("stream") || !j.contains("data"))
                    return;

                const auto& d = j["data"];
                std::string symbol = d.value("s", "");
                if (symbol.empty()) return;


                const auto& bids = d["b"];
                const auto& asks = d["a"];

                L2OrderBook book;

                book.bids.reserve(bids.size());
                book.asks.reserve(asks.size());

                for (auto& lvl : bids) {
                    book.bids.push_back({
                        std::stod(lvl[0].get<std::string>()),
                        std::stod(lvl[1].get<std::string>())
                    });
                }

                for (auto& lvl : asks) {
                    book.asks.push_back({
                        std::stod(lvl[0].get<std::string>()),
                        std::stod(lvl[1].get<std::string>())
                    });
                }


                {
                    std::lock_guard<std::mutex> lock(books_mutex_);
                    books_[symbol] = book;
                }


            } catch (const std::exception& e) {
                std::cerr << "❌ MarkPrice parse error: " << e.what() << std::endl;
                _mark_price_logger.str("");
                _mark_price_logger.clear();
                _mark_price_logger << "❌ MarkPrice parse error: " << e.what();
                Logger::error(_mark_price_logger.str());
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
                                        std::string msg =
                        "⏱️ Signal-to-order latency for " + symbol +
                        " = " + std::to_string(latency_us) +
                        " µs (" + std::to_string(latency_us / 1000.0) +
                        " ms)";
                    tg_worker.push(msg);
                    market_order(side,symbol,amount,price);
                    // algo_orders(side,symbol,amount,price);
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
        tg_worker.push(message);
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
        } catch (const std::exception& e) {
            std::cerr << "❌ query_position error: " << e.what() << std::endl;
        }
    });
}


void MonitorTrades::algo_SL_orders(const std::string &side, const std::string &symbol, double quantity, double entry_price, double sl) {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, skipping close order.\n";
        return;
    }
    std::string pos_side;
    static const std::unordered_map<std::string, double> quantity_map = {
        {"1000satsusdt", 35000000},
        {"jellyjellyusdt", 7000},
        {"gtcusdt", 1200},
        {"flmusdt", 10000},
        {"labusdt", 250},
        {"coaiusdt", 120},
        {"evaausdt", 300},
        {"pippinusdt", 300},


    };

    if (side == "LONG") {
        pos_side = "SELL";
    } else if (side == "SHORT") {
        pos_side = "BUY";
    }
    logger.info("Algo order params: \nSymbol: " + symbol + "\nSide: " + side + "\nQuantity: " +
        std::to_string(quantity) + "\nEntry Price: " + std::to_string(entry_price));

    long long ts = current_timestamp_ms();
    std::map<std::string, std::string> params = {
        {"algoType", "CONDITIONAL"},
        {"apiKey", API_KEY},
        {"symbol", symbol},
        {"side", pos_side},
        {"positionSide", "BOTH"},
        {"closePosition", "true"},
        {"type", "STOP_MARKET"},
        {"triggerPrice", std::to_string(sl)},   // ← REQUIRED!!!
        {"timestamp", std::to_string(ts)}
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
                  << " entry=" << entry_price
                  << " (locked for confirmation)\n";
    } catch (std::exception& e) {
        std::cerr << "❌ Error sending close_trade: " << e.what() << std::endl;
    }

}



void MonitorTrades::algo_TP_orders(const std::string &side, const std::string &symbol,
    double quantity, double entry_price,
double tp) {
    if (!connected_) {
        std::cerr << "⚠️ Not connected, skipping close order.\n";
        return;
    }

    L2OrderBook book;
    std::string pos_side;
    static const std::unordered_map<std::string, double> quantity_map = {
        {"1000satsusdt", 35000000},
        {"jellyjellyusdt", 7000},
        {"gtcusdt", 1200},
        {"flmusdt", 10000},
        {"labusdt", 250},
        {"coaiusdt", 120},
        {"evaausdt", 300},
        {"pippinusdt", 300},


    };

    if (side == "LONG") {
        pos_side = "SELL";
    } else if (side == "SHORT") {
        pos_side = "BUY";
    }
    logger.info("Algo order params: \nSymbol: " + symbol + "\nSide: " + side + "\nQuantity: " +
        std::to_string(quantity) + "\nEntry Price: " + std::to_string(entry_price));


    long long ts = current_timestamp_ms();
    std::map<std::string, std::string> params = {
        {"algoType", "CONDITIONAL"},
        {"apiKey", API_KEY},
        {"symbol", symbol},
        {"side", pos_side},
        {"positionSide", "BOTH"},
        {"closePosition", "true"},
        {"type", "TAKE_PROFIT_MARKET"},
        {"triggerPrice", std::to_string(tp)},   // ← REQUIRED!!!
        {"timestamp", std::to_string(ts)}
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
                  << " entry=" << entry_price
                  << " (locked for confirmation)\n";
    } catch (std::exception& e) {
        std::cerr << "❌ Error sending close_trade: " << e.what() << std::endl;
    }

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
        {"1000satsusdt", 35000000},
        {"jellyjellyusdt", 7000},
        {"gtcusdt", 1200},
        {"flmusdt", 10000},
        {"labusdt", 250},
        {"coaiusdt", 120},
        {"evaausdt", 300},
        {"pippinusdt", 300},


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
        std::cout << "📤 Sent  order: "
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
                    logger.info(r.dump());
                    if (r.contains("symbol") && r.contains("algoStatus")) {
                        std::string sym    = r.value("symbol", "");
                        std::string status = r.value("algoStatus", "");
                        if (status == "FILLED" || status == "PARTIALLY_FILLED" || status == "CANCELED") {
                            if (closing_trades_.erase(sym))
                                std::cout << "✅ Order fill confirmed, unlocking " << sym << std::endl;
                        }
                    }
                }
                // 🧩 2️⃣ Handle position snapshots
                if (j.contains("result") && j["result"].is_array()) {
                    std::unordered_set<std::string> snapshot_symbols;
                    logger.info(j.dump());
                    for (const auto& pos : j["result"]) {
                        std::string symbol   = pos.value("symbol", "");
                        double entry         = std::stod(pos.value("entryPrice", "0"));
                        double posAmt        = std::stod(pos.value("positionAmt", "0"));
                        double markPrice     = std::stod(pos.value("markPrice", "0"));

                        if (symbol.empty()) continue;
                        auto it = active_trades_.find(symbol);

                        if (it != active_trades_.end()) {


                            if (posAmt == 0) {
                                std::cout << "✔️ Position closed on Binance: " << symbol << std::endl;
                                active_trades_.erase(it);
                                closing_trades_.erase(symbol);
                                send_confirmation("CLOSED " + symbol);
                                continue;
                            }

                            ActiveTrade &t = it->second;

                            double entry = t.entry;
                            double exit = markPrice;
                            double pnl = 0.0;


                            if (t.side == "LONG") {
                                pnl = (exit - entry) / entry * 100.0;
                            } else {
                                pnl = (entry - exit) / entry * 100.0;
                            }
                            std::string reason = (exit >= t.tp) ? "🎯 TP HIT" : "🛑 SL HIT";
                            std::string msg =
                                reason + " " + symbol +
                                "\nEntry: " + std::to_string(entry) +
                                "\nExit: " + std::to_string(exit) +
                                "\nTP: " + std::to_string(t.tp) +
                                "\nSL: " + std::to_string(t.sl) +
                                "\nPnL: " + std::to_string(pnl) + "%" +
                                "\nWallet: " + std::to_string(WALLET + pnl);

                            tg_worker.push(msg);
                            Logger::info(msg);
                            send_confirmation("CLOSED " + symbol);

                            WALLET += pnl;

                            // Remove from active and closing lists
                            active_trades_.erase(it);
                            closing_trades_.erase(symbol);

                            std::cout << "📌 Closed via conditional order: " << symbol
                                      << " PnL=" << pnl << "%" << std::endl;

                            continue;  // skip further logic
                        }

                        snapshot_symbols.insert(symbol);

                        std::string side = posAmt > 0 ? "LONG" : "SHORT";

                        algo_TP_orders(side, symbol, std::abs(posAmt), entry);
                        algo_SL_orders(side, symbol, std::abs(posAmt), entry);

                        // if (posAmt == 0) {
                        //     std::cout << "✅ " << symbol << " confirmed closed (positionAmt=0)\n";
                        //     closing_trades_.erase(symbol);
                        //     active_trades_.erase(symbol);
                        //     continue;
                        // }


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
                        L2OrderBook book;
                        {
                            std::lock_guard<std::mutex> lock(books_mutex_);
                            if (!books_.count(symbol)) {
                                // fallback: static TP/SL until OB arrives
                                auto [tp, sl] = calculate_sl_tp(t.side, entry, symbol);
                                t.tp = tp;
                                t.sl = sl;
                                goto done_tpsl;
                            }
                            book = books_[symbol];
                        }

                        // determine tick size + ATR (or use fallback)
                        double tick = 0.0001;
                        double atr  =  entry * 0.001; // fallback if you don’t compute ATR

                        TPSL tpsl;
                        if (t.side == "LONG")
                            tpsl = calc_tp_sl_long(book, entry, tick, atr);
                        else
                            tpsl = calc_tp_sl_short(book, entry, tick, atr);

                        t.tp = tpsl.tp;
                        t.sl = tpsl.sl;

                        done_tpsl:;

                        t.open_time = std::chrono::steady_clock::now();  // ✅ set first
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