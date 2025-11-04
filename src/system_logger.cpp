//
// Created by Gor Barseghyan on 03.11.25.
//
#include "system_logger.hpp"

std::string Logger::timestamp_now() {
    std::ostringstream oss;
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::ofstream& Logger::file_stream() {
    static std::ofstream file;
    static std::string current_date;

    // daily rotation
    std::ostringstream date_oss;
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    date_oss << std::put_time(&tm, "%Y-%m-%d");
    std::string today = date_oss.str();

    if (!file.is_open() || current_date != today) {
        if (file.is_open()) file.close();
        std::filesystem::create_directory("logs");
        std::string filename = "logs/trades_" + today + ".log";
        file.open(filename, std::ios::app);
        current_date = today;
    }
    return file;
}

std::mutex& Logger::log_mutex() {
    static std::mutex mtx;
    return mtx;
}

void Logger::info(const std::string& msg) {
    std::lock_guard<std::mutex> lock(log_mutex());
    std::string ts = timestamp_now();
    std::cout << "[INFO] " << ts << " | " << msg << std::endl;
    file_stream() << "[INFO] " << ts << " | " << msg << std::endl;
}

void Logger::error(const std::string& msg) {
    std::lock_guard<std::mutex> lock(log_mutex());
    std::string ts = timestamp_now();
    std::cerr << "[ERROR] " << ts << " | " << msg << std::endl;
    file_stream() << "[ERROR] " << ts << " | " << msg << std::endl;
}

void Logger::trade(const std::string& symbol,
                   const std::string& action,
                   double entry, double exit,
                   double pnl) {
    std::lock_guard<std::mutex> lock(log_mutex());
    std::string ts = timestamp_now();
    std::ofstream& file = file_stream();
    file << std::fixed << std::setprecision(6);
    file << "[TRADE] " << ts
         << " | " << symbol
         << " | " << action
         << " | Entry=" << entry
         << " | Exit=" << exit
         << " | PnL=" << pnl
         << std::endl;

    std::cout << "[TRADE] " << ts
              << " | " << symbol
              << " | " << action
              << " | Entry=" << entry
              << " | Exit=" << exit
              << " | PnL=" << pnl
              << std::endl;
}