#pragma once
#include <queue>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "telegram.hpp"

class TelegramWorker {
public:
    TelegramWorker() {
        worker = std::thread([this] { loop(); });
        worker.detach();
    }

    void push(const std::string& msg) {
        std::lock_guard<std::mutex> lock(m);
        q.push(msg);
        cv.notify_one();
    }

private:
    Telegram telegram;
    std::queue<std::string> q;
    std::mutex m;
    std::condition_variable cv;
    std::thread worker;

    void loop() {
        while (true) {
            std::unique_lock<std::mutex> lock(m);
            cv.wait(lock, [&]{ return !q.empty(); });

            std::string msg = q.front();
            q.pop();
            lock.unlock();

            telegram.send_msg(msg);
        }
    }
};