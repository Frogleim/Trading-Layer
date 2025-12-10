#include "file_monitoring.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>
#include <algorithm> // for std::transform
#include <iomanip>   // for std::put_time
#include <chrono>
#include <mutex>

const std::string ACTIVE_FILE = "active_trades.txt";
const std::string LOG_FILE = "log.txt";
std::mutex file_mtx;



void load_env(const std::string& path) {
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string key = line.substr(0, pos);
        std::cout << key << std::endl;
        std::string value = line.substr(pos + 1);
        setenv(key.c_str(), value.c_str(), 1);
    }
}


auto parse_double = [](const std::string& val, double default_value = 0.0) -> double {
    if (val.empty()) return default_value;
    try {
        return std::stod(val);
    } catch (const std::exception& e) {
        std::cerr << "⚠️ Invalid double in env: " << val
                  << " (" << e.what() << ")" << std::endl;
        return default_value;
    }
};


EnvData load_config(const std::string& path) {
    load_env(path);

    auto safe_getenv = [](const char* name) -> std::string {
        const char* val = std::getenv(name);
        if (!val) {
            std::cerr << "⚠️ Missing env variable: " << name << std::endl;
            return "";
        }
        return val;
    };

    // Helper for boolean parsing
    auto parse_bool = [](const std::string& val, bool default_value = false) -> bool {
        std::string lower = val;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        if (lower == "1" || lower == "true" || lower == "yes" || lower == "on")
            return true;
        if (lower == "0" || lower == "false" || lower == "no" || lower == "off")
            return false;
        return default_value;
    };

    EnvData cfg;
    cfg.is_testnet      = parse_bool(safe_getenv("IS_TESTNET"), false);
    cfg.test_api_key    = safe_getenv("TEST_API_KEY");
    cfg.test_api_secret = safe_getenv("TEST_API_SECRET");
    cfg.api_key         = safe_getenv("API_KEY");
    cfg.api_secret      = safe_getenv("API_SECRET");
    cfg.base_url        = safe_getenv("BASE_URL");
    cfg.test_base_url   = safe_getenv("TEST_BASE_URL");
    cfg.market_data     = safe_getenv("MARKET_DATA");
    cfg.TP              = parse_double(safe_getenv("TP"), 0.0);
    cfg.SL              = parse_double(safe_getenv("SL"), 0.0);
    cfg.pos_amt         = parse_double(safe_getenv("POS_SIZE"), 0.0);
    cfg.telegram_token  = safe_getenv("TELEGRAM_TOKEN");
    cfg.chat_id         = safe_getenv("CHAT_ID");
    cfg.thread_id       = safe_getenv("THREAD_ID");

    return cfg;
}





std::string to_lowercase(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return result;
}

bool symbol_exists_in_file(const std::string& symbol) {
    std::lock_guard<std::mutex> lock(file_mtx);
    std::ifstream in(ACTIVE_FILE);
    std::string line;
    while (std::getline(in, line)) {
        if (line == symbol) return true;
    }
    return false;
}


auto LOG_TIME = std::chrono::system_clock::now();

std::string format_log_time(std::chrono::system_clock::time_point tp) {
    std::time_t t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&t);
    std::ostringstream oss;
    oss << std::put_time(&tm, "[%Y-%m-%d %H:%M:%S]");
    return oss.str();
}


void write_log(const std::string& msg) {
    // check existence WITHOUT holding lock (to avoid recursive lock)
    std::ofstream ensure(LOG_FILE, std::ios::app);
    ensure.close();

    std::lock_guard<std::mutex> lock(file_mtx);
    std::ofstream out(LOG_FILE, std::ios::app);
    if (!out.is_open()) {
        std::cerr << "❌ Cannot open " << LOG_FILE << " for writing\n";
        return;
    }
    out << format_log_time(LOG_TIME) << msg << "\n";
    std::cout << "📄 Added symbol: " << msg << " → " << LOG_FILE << std::endl;
}

void add_symbol_to_file(const std::string& symbol) {
    // check existence WITHOUT holding lock (to avoid recursive lock)
    std::ofstream ensure(ACTIVE_FILE, std::ios::app);
    ensure.close();
    if (symbol_exists_in_file(symbol)) return;

    std::lock_guard<std::mutex> lock(file_mtx);
    std::ofstream out(ACTIVE_FILE, std::ios::app);
    if (!out.is_open()) {
        std::cerr << "❌ Cannot open " << ACTIVE_FILE << " for writing\n";
        return;
    }
    out << symbol << "\n";
    std::cout << "📄 Added symbol: " << symbol << " → " << ACTIVE_FILE << std::endl;
}

void remove_symbol_from_file(const std::string& symbol) {

    std::string lower_symbol = to_lowercase(symbol);
    std::lock_guard<std::mutex> lock(file_mtx);
    std::ifstream in(ACTIVE_FILE);
    if (!in.is_open()) return;

    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (line != lower_symbol && !line.empty()) {
            lines.push_back(line);
        }
    }
    in.close();

    std::ofstream out(ACTIVE_FILE, std::ios::trunc);
    for (const auto& l : lines) out << l << "\n";
    std::cout << "🧹 Removed symbol: " << lower_symbol << " from " << ACTIVE_FILE << std::endl;
}

std::vector<std::string> load_symbols_from_file() {
    std::lock_guard<std::mutex> lock(file_mtx);
    std::ifstream in(ACTIVE_FILE);
    std::vector<std::string> symbols;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) symbols.push_back(line);
    }
    return symbols;
}



