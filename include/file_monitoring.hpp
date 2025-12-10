//
// Created by Gor Barseghyan on 12.10.25.
//

#pragma once
#include <string>
#include <vector>
#include <mutex>



struct EnvData {
    std::string api_key;
    std::string api_secret;
    std::string base_url;
    std::string test_api_key;
    std::string test_api_secret;
    std::string test_base_url;
    std::string market_data;
    bool is_testnet;
    double TP;
    double SL;
    double pos_amt;
    std::string telegram_token;
    std::string chat_id;
    std::string thread_id;
};


extern std::mutex file_mtx;  // Declare global mutex

void load_env(const std::string& path);
EnvData load_config(const std::string& path = ".env");

void add_symbol_to_file(const std::string& symbol);
void remove_symbol_from_file(const std::string& symbol);
bool symbol_exists_in_file(const std::string& symbol);
void write_log(const std::string& msg);
std::vector<std::string> load_symbols_from_file();
