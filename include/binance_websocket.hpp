#pragma once
#include <boost/asio.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>
#include <string>

double calculate_sl_tp();

struct PositionInfo {
    bool active = false;
    std::string direction;
    double entry = 0.0;
    double unrealized_pnl = 0.0;
    double realized_pnl = 0.0;
    int total_trades = 0;
    int open_trades = 0;
};



struct Positions {
    std::string symbol;
    std::string side;
    double quantity;
};

class MonitorTrades {
public:
    static const std::string API_KEY;
    static const std::string API_SECRET;
    static const std::string HOST;
    static const std::string PORT;
    static const std::string TARGET;

    MonitorTrades();
    ~MonitorTrades();
    void connect();
    void query_position();
    void start_async_read();
    void run_event_loop();
    void market_order(const std::string& side, const std::string& symbol, double quantity, double entry_price);
    void display_dashboard();


    template<typename F>
    void post(F&& fn) {
        boost::asio::post(io_ctx_, std::forward<F>(fn));
    }

private:
    boost::asio::io_context io_ctx_;   // must come before resolver_ and ws_
    boost::asio::ssl::context ssl_ctx_; // must come before ws_

    boost::asio::ip::tcp::resolver resolver_;
    boost::beast::websocket::stream<
        boost::asio::ssl::stream<boost::asio::ip::tcp::socket>> ws_;

    using WorkGuard = boost::asio::executor_work_guard<
        boost::asio::io_context::executor_type>;
    WorkGuard work_guard_;

    boost::beast::flat_buffer buffer_;
    bool connected_ = false;

    std::unordered_map<std::string, bool> close_sent_;
    std::map<std::string, std::vector<Positions>> positions_;
    std::map<std::string, double> active_trades_;
    std::unordered_map<std::string, bool> closing_trades_;
};

inline std::map<std::string, PositionInfo> active_positions;
inline bool g_market_wss_alive = false;
inline bool g_trading_wss_alive = false;
inline std::mutex mtx;
inline std::mutex log_mutex;
inline std::mutex io_mutex;
inline std::vector<std::string> recent_logs;

inline auto START_TIME = std::chrono::system_clock::now();