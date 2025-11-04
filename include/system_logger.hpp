#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <iomanip>
#include <ctime>
#include <filesystem>
#include <mutex>

class Logger {
public:
    static void info(const std::string& msg);
    static void error(const std::string& msg);
    static void trade(const std::string& symbol,
                      const std::string& action,
                      double entry, double exit,
                      double pnl);

private:
    static std::string timestamp_now();
    static std::ofstream& file_stream();
    static std::mutex& log_mutex();
};