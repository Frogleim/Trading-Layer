#pragma once
#include <string>
#include <deque>
#include <mutex>

// ========== GLOBAL DASHBOARD LOGGER ==========
extern std::mutex log_mutex;
extern std::deque<std::string> recent_logs;
extern std::atomic<bool> g_market_wss_alive;
extern std::atomic<bool> g_trading_wss_alive;
extern const size_t MAX_LOG_LINES;

// Global function accessible from anywhere
void log_to_dashboard(const std::string& message);