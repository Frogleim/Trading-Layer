//
// Created by Gor Barseghyan on 28.10.25.
//


#include <iostream>
#include "binance_websocket.hpp"


int main() {
    try {
        auto trader = std::make_shared<MonitorTrades>();

        trader -> connect();

        trader->start_async_read();
        std::thread io_thread([&]() {
            trader->run_event_loop();
        });

        while (true) {
            trader->query_position();       // request position snapshot
            std::this_thread::sleep_for(std::chrono::seconds(5));  // repeat every 5s

            // Optional: monitor connection health
            // if (!trader->is_connected()) {
            //     std::cerr << "⚠️ Connection lost, attempting reconnect...\n";
            //     trader->connect();
            //     trader->start_async_read();
            // }
        }

        io_thread.join();
    }
    catch (const std::exception& e) {
        std::cerr << "❌ Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}