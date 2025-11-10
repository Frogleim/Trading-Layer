#pragma once


#include <iostream>
#include "binance_websocket.hpp"

std::string timestamp_now();

void log_trade_csv(const ActiveTrade& trade,
                   const std::string& symbol,
                   const std::string& action,
                   double exit_price,
                   double trade_pnl);


void append_trade_to_csv(const std::string& symbol,
                         const std::string& side,
                         double entry,
                         double tp,
                         double sl,
                         double close_price,
                         const std::string& reason,
                         double pnl,
                         long latency_us);