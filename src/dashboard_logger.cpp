//
// Created by Gor Barseghyan on 15.10.25.
//
#include "dashboard_logger.hpp"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>


std::mutex log_mutex;
std::deque<std::string> recent_logs;
const size_t MAX_LOG_LINES = 10;
std::atomic<bool> g_market_wss_alive(false);
std::atomic<bool> g_trading_wss_alive(false);

void log_to_dashboard(const std::string& message) {
    std::lock_guard<std::mutex> lock(log_mutex);
    std::ostringstream oss;

    // format timestamp
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    oss << "[" << std::put_time(std::localtime(&now), "%H:%M:%S") << "] " << message;

    // store log
    recent_logs.push_back(oss.str());
    if (recent_logs.size() > MAX_LOG_LINES)
        recent_logs.pop_front();
}


