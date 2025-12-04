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



std::string env_path = ".env";
EnvData env = load_config(env_path);

const std::string MonitorTrades::API_KEY    = env.is_testnet ? env.test_api_key    : env.api_key;
const std::string MonitorTrades::API_SECRET = env.is_testnet ? env.test_api_secret : env.api_secret;
const std::string MonitorTrades::HOST       = env.is_testnet ? env.test_base_url   : env.base_url;
const std::string MonitorTrades::MARK_PRICE_HOST = env.market_data;

const std::string MonitorTrades::PORT   = "443";
const std::string MonitorTrades::TARGET = "/ws-fapi/v1";  // private WebSocket endpoint
const double TP = env.TP;
const double SL = env.SL;
constexpr double SL_BUFFER = 0.001;



namespace {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);
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

