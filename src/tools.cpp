//
// Created by Gor Barseghyan on 21.10.25.
//
#include "tools.hpp"

#include "file_monitoring.hpp"
#include "zmq_publisher.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <openssl/hmac.h>
#include <uuid/uuid.h>
#include <chrono>
#include <thread>
#include <map>
#include <iomanip>
#include <sstream>
#include <iostream>


constexpr double WALL_MULTIPLIER = 3.0;

// === Utility ===
long long current_timestamp_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

double find_next_wall(const std::vector<Level>& asks) {
    // compute avg liquidity
    double avg = 0;
    for (auto &lvl : asks) avg += lvl.size;
    avg /= asks.size();

    double threshold = avg * WALL_MULTIPLIER;

    for (auto &lvl : asks) {
        if (lvl.size >= threshold) return lvl.price;
    }
    return asks.back().price; // fallback
}


double find_liquidity_void(const std::vector<Level>& bids) {
    double avg = 0;
    for (auto &lvl : bids) avg += lvl.size;
    avg /= bids.size();

    double threshold = avg * 0.30; // <30% of avg = void

    for (auto &lvl : bids) {
        if (lvl.size <= threshold)
            return lvl.price;
    }
    return bids.back().price;
}


double calc_obi(const L2OrderBook& book) {
    double bid_sum = 0;
    double ask_sum = 0;

    for (auto& b : book.bids) bid_sum += b.size;
    for (auto& a : book.asks) ask_sum += a.size;

    return (bid_sum - ask_sum) / (bid_sum + ask_sum);
}

inline double get_spread(const L2OrderBook& ob) {
    return ob.asks[0].price - ob.bids[0].price;
}


double limit_tp_to_volatility(double entry, double tp, double atr) {
    double max_move = atr * 0.5;  // allow up to 0.5 ATR
    return std::min(tp, entry + max_move);
}

double limit_sl_to_volatility(double entry, double sl, double atr) {
    double max_loss = atr * 0.25;
    return std::max(sl, entry - max_loss);
}



TPSL calc_tp_sl_long(
    const L2OrderBook& book,
    double entry,
    double tick,
    double atr
){
    // 1. Detect liquidity-based TP
    double wall_price = find_next_wall(book.asks);
    double tp = wall_price - (2 * tick); // exit before wall

    // 2. Limit TP by volatility
    tp = limit_tp_to_volatility(entry, tp, atr);

    // 3. Detect liquidity void for SL
    double void_price = find_liquidity_void(book.bids);
    double sl = void_price + (2 * tick); // place above void

    // 4. Ensure SL is not inside spread
    double spread = get_spread(book);
    if (entry - sl < spread)
        sl = entry - (spread * 1.5);

    // 5. Volatility guard
    sl = limit_sl_to_volatility(entry, sl, atr);

    return {tp, sl};
}


TPSL calc_tp_sl_short(
    const L2OrderBook& book,
    double entry,
    double tick,
    double atr
){
    // ============
    // 1. SHORT TP
    //  → Take profit where BUYERS vanish / big bid wall
    // ============
    double wall_price = find_next_wall(book.bids);  // bids, not asks
    double tp = wall_price + (2 * tick);            // exit before wall (above)

    // Limit by volatility (move downwards)
    double max_move = atr * 0.5;
    if (entry - tp > max_move)
        tp = entry - max_move;


    // ============
    // 2. SHORT SL
    //  → Stop loss where SELLERS disappear (ask void)
    // ============
    double void_price = find_liquidity_void(book.asks); // asks, not bids
    double sl = void_price - (2 * tick);                // SL below void


    // ============
    // 3. Spread protection (SHORT)
    // SL must NOT be inside the spread
    // Spread = ask - bid
    // Short loses money if price goes UP
    // So we need: sl > entry + spread
    // ============
    double spread = get_spread(book);

    if (sl - entry < spread)
        sl = entry + (spread * 1.5);


    // ============
    // 4. Volatility guard (SHORT)
    // limit max upward SL move
    // ============
    double max_loss = atr * 0.25;
    if (sl - entry > max_loss)
        sl = entry + max_loss;


    return {tp, sl};
}

std::string generate_uuid() {
    uuid_t uuid;
    uuid_generate_random(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    return std::string(uuid_str);
}

std::string to_upper_symbol(const std::string& symbol) {
    std::string upper = symbol;
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return upper;
}

std::string generate_signature(const std::map<std::string, std::string>& params,
                               const std::string& secret) {
    std::ostringstream query;
    bool first = true;
    for (const auto& [k, v] : params) {
        if (!first) query << "&";
        query << k << "=" << v;
        first = false;
    }

    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    HMAC(EVP_sha256(), secret.c_str(), secret.size(),
         reinterpret_cast<const unsigned char*>(query.str().c_str()),
         query.str().size(), digest, &len);

    std::ostringstream hex;
    for (unsigned int i = 0; i < len; ++i)
        hex << std::hex << std::setw(2) << std::setfill('0') << (int)digest[i];

    return hex.str();
}

std::string format_time(std::chrono::system_clock::time_point tp) {
    std::time_t t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&t);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}