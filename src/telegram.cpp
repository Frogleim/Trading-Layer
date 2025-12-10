//
// Created by Gor Barseghyan on 25.11.25.
//

#include "telegram.hpp"

#include <iostream>

#include "file_monitoring.hpp"
#include <curl/curl.h>
#include <__ostream/basic_ostream.h>


std::string env_paths = ".env";
EnvData env_values = load_config(env_paths);


std::string telegram_token = env_values.telegram_token;
std::string chat_id = env_values.chat_id;
std::string thread_id = env_values.thread_id;

void Telegram::send_msg(const std::string& msg) {
    std::cout << "Chat ID" << chat_id << std::endl;
    CURL* curl = curl_easy_init();

    if (!curl) {
        std::cerr << "Failed to initialize curl" << std::endl;
        return;
    }

    std::string base_url = "https://api.telegram.org/bot" + telegram_token + "/sendMessage";

    std::string json_payload = "{"
    "\"chat_id\": \"" + chat_id + "\","
    "\"text\": \"" + msg + "\","
    "\"message_thread_id\": " + thread_id + ","
    "\"parse_mode\": \"HTML\""
"}";

    struct curl_slist* headers = nullptr;

    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, base_url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_payload.c_str());

    // For debugging: print response to stdout
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
        +[](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
            std::string data(ptr, size * nmemb);
            std::cout << "Response: " << data << std::endl;
            return size * nmemb;
        }
    );

    // Perform request
    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        std::cerr << "Curl error: " << curl_easy_strerror(res) << std::endl;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);





}