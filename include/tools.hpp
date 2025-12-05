#pragma once


#include <iostream>
#include <map>
#include <chrono>



long long current_timestamp_ms();
std::string current_timestamp();

std::string generate_signature(const std::map<std::string, std::string>& params,
                               const std::string& secret);


std::string generate_uuid();
std::string format_time(std::chrono::system_clock::time_point tp);
std::string to_upper_symbol(const std::string& symbol);


std::string to_lower_symbol(const std::string& symbol);