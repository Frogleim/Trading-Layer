#pragma once

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/core.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <map>
#include <unordered_map>
#include <vector>
#include <memory>
#include <mutex>
#include <zmq.hpp>
#include "zmq_publisher.hpp"

// Aliases
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;
using ssl_stream = ssl::stream<tcp::socket>;
using websocket_t = websocket::stream<ssl_stream>;

// ================= Utility structs =================
struct PositionInfo {
    bool active = false;
    std::string direction;
    double entry = 0.0;
    double unrealized_pnl = 0.0;
    double realized_pnl = 0.0;
    int total_trades = 0;
    int open_trades = 0;
};

struct ActiveTrade {
    std::string side;   // "LONG" or "SHORT"
    double entry = 0.0;
    double amount = 0.0;
    double tp = 0.0;
    double sl = 0.0;
};

struct Positions {
    std::string symbol;
    std::string side;
    double quantity = 0.0;
};

// ================= MonitorTrades =================
class MonitorTrades {
public:
    // --- Static config ---
    static const std::string API_KEY;
    static const std::string API_SECRET;
    static const std::string HOST;
    static const std::string MARK_PRICE_HOST;
    static const std::string TEST_HOST;
    static const std::string PORT;
    static const std::string TARGET;
    static const std::string TEST_API_KEY;
    static const std::string TEST_API_SECRET;
    static const double TP;
    static const double SL;

    // --- Lifecycle ---
    MonitorTrades();
    ~MonitorTrades();

    void connect();
    void query_position();
    void start_async_read();
    void start_markprice_read();
    void start_zmq_listener();
    void send_confirmation(std::string symbol);
    void market_order(const std::string& side,
                      const std::string& symbol,
                      double quantity,
                      double entry_price);
    void run_event_loop();

    template<typename F>
    void post(F&& fn) {
        net::post(io_private_, std::forward<F>(fn));
    }

private:
    // --- ZMQ ---
    zmq::context_t zmq_ctx_{1};
    zmq::socket_t zmq_pub_;
    std::thread zmq_thread_;
    std::mutex zmq_mutex_;
    // --- Independent I/O contexts ---
    net::io_context io_private_;
    net::io_context io_mark_;

    // --- Threads for each IO loop ---
    std::thread thread_private_;
    std::thread thread_mark_;

    // --- Work guards ---
    using WorkGuard = net::executor_work_guard<net::io_context::executor_type>;
    WorkGuard work_guard_private_;
    WorkGuard work_guard_mark_;

    // --- SSL context ---
    ssl::context ssl_ctx_;

    // --- Resolvers (separate per IO) ---
    tcp::resolver resolver_private_;
    tcp::resolver resolver_mark_;

    // --- WebSockets ---
    std::unique_ptr<websocket_t> ws_;        // private / trading WS
    std::unique_ptr<websocket_t> ws_mark_;   // markPrice WS

    // --- Buffers ---
    beast::flat_buffer buffer_;

    // --- Trade state ---
    bool connected_ = false;
    std::unordered_map<std::string, bool> closing_trades_;
    std::unordered_map<std::string, ActiveTrade> active_trades_;
    std::unordered_map<std::string, double> latest_mark_prices_;

    // --- Helpers ---
    ZMQComm zmq;
};

// ================= Global shared state =================
inline std::map<std::string, PositionInfo> active_positions;
inline std::mutex log_mutex;
inline std::vector<std::string> recent_logs;
inline auto START_TIME = std::chrono::system_clock::now();
