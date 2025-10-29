#include <iostream>
#include <thread>
#include "binance_websocket.hpp"
#include "zmq_publisher.hpp"

int main() {
    try {
        auto trader = std::make_shared<MonitorTrades>();

        // ✅ Connect to Binance (private + markPrice)
        trader->connect();

        // ✅ Start ZMQ listener for signals
        trader->start_zmq_listener();

        // ✅ Start periodic position checks in a background thread
        std::thread position_thread([&]() {
            while (true) {
                trader->query_position();
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        });

        // ✅ Keep main thread alive with I/O event loops
        trader->run_event_loop();

        position_thread.join(); // should never reach here normally
    }
    catch (const std::exception& e) {
        std::cerr << "❌ Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}