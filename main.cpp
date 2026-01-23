#include <iostream>
#include <thread>
#include <memory>
#include <chrono>
#include <nlohmann/json.hpp>

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
            try {
                while (true) {
                    trader->query_position();
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                }
            }
            catch (const nlohmann::json::exception& e) {
                std::cerr << "🔥 JSON error in position thread: "
                          << e.what() << std::endl;
                std::abort();
            }
            catch (const std::exception& e) {
                std::cerr << "🔥 Exception in position thread: "
                          << e.what() << std::endl;
                std::abort();
            }
        });

        // ✅ Run main event loop (websockets)
        trader->run_event_loop();

        position_thread.join(); // normally unreachable
    }
    catch (const nlohmann::json::exception& e) {
        std::cerr << "🔥 Fatal JSON error: " << e.what() << std::endl;
        return 1;
    }
    catch (const std::exception& e) {
        std::cerr << "❌ Fatal std::exception: " << e.what() << std::endl;
        return 1;
    }
    catch (...) {
        std::cerr << "💥 Unknown fatal error occurred" << std::endl;
        return 1;
    }

    return 0;
}
