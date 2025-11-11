//
// Created by Gor Barseghyan on 21.10.25.
//
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



// === Utility ===
long long current_timestamp_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
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