//
// Created by Gor Barseghyan on 01.11.25.
//
#include <iostream>
#include "binance_websocket.hpp"
#include <fstream>
#include <filesystem>
#include <iomanip>        // ✅ for std::quoted, std::setprecision, std::fixed
#include <chrono>

std::string timestamp_now() {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string ts = std::ctime(&now);
    if (!ts.empty()) ts.pop_back();
    return ts;
}


void append_trade_to_csv(const std::string& symbol,
                         const std::string& side,
                         double entry,
                         double tp,
                         double sl,
                         double close_price,
                         const std::string& reason,
                         double pnl,
                         long latency_us)
{
    namespace fs = std::filesystem;
    const std::string filename = "trades_log.csv";
    bool file_exists = fs::exists(filename);

    std::ofstream file(filename, std::ios::app);
    if (!file.is_open()) {
        std::cerr << "❌ Failed to open " << filename << " for writing.\n";
        return;
    }

    if (!file_exists) {
        file << "timestamp,symbol,side,entry,tp,sl,close_price,reason,pnl,latency_us\n";
    }

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    file << std::put_time(std::gmtime(&now), "%Y-%m-%d %H:%M:%S") << ","
         << symbol << "," << side << ","
         << entry << "," << tp << "," << sl << ","
         << close_price << "," << reason << ","
         << pnl << "," << latency_us
         << "\n";

    file.close();
}

void log_trade_csv(const ActiveTrade& trade,
                   const std::string& symbol,
                   const std::string& action,
                   double  entry_price,
                   double exit_price)
{
    const std::string filename = "trade_results.csv";
    bool file_exists = std::filesystem::exists(filename);

    std::ofstream file(filename, std::ios::app);
    if (!file.is_open()) {
        std::cerr << "❌ Failed to open " << filename << " for writing.\n";
        return;
    }

    // Write header if file doesn't exist
    if (!file_exists) {
        file << "Timestamp,Symbol,Action,Direction,Entry,Exit,TP,SL,TradePnL,Amount\n";
    }

    // Write one trade record
    file << std::quoted(timestamp_now()) << ','   // timestamp
         << symbol << ','                         // symbol
         << action << ','                         // OPEN / CLOSE
         << trade.side << ','                     // LONG / SHORT
         << std::fixed << std::setprecision(6)
         << trade.entry << ','                    // entry price
         << exit_price << ','                     // exit price
         << trade.tp << ','                       // take profit
         << trade.sl << ','                       // stop loss
         << trade.amount << '\n';                 // position size
}




