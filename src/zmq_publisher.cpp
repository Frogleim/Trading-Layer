#include "zmq_publisher.hpp"
#include "dashboard_logger.hpp"
#include <queue>

zmq::context_t exec_ctx(1);
zmq::socket_t exec_sub(exec_ctx, zmq::socket_type::sub);
zmq::socket_t exec_pub(exec_ctx, zmq::socket_type::pub);
// ack server sockets for guaranteed delivery
zmq::socket_t ack_rep(exec_ctx, zmq::socket_type::rep);
std::mutex ack_mtx;
std::queue<std::string> ack_queue;
zmq::context_t ctx(1);
using json = nlohmann::json;

// === Initialize execution ZMQ sockets ===
void init_zmq_execution(int sub_port, int pub_port) {
    std::ostringstream sub_addr, pub_addr;
    // Allow MONITOR host to be provided via env for container deployments
    const char* monitor_host_env = std::getenv("MONITOR_PUB_HOST");
    std::string monitor_host = monitor_host_env ? monitor_host_env : "127.0.0.1";
    sub_addr << "tcp://" << monitor_host << ":" << sub_port;
    pub_addr << "tcp://*:" << pub_port;

    exec_sub.connect(sub_addr.str());
    // use modern cppzmq API
    exec_sub.set(zmq::sockopt::subscribe, "");

    exec_pub.bind(pub_addr.str());

    std::cout << "🚀 Execution connected to " << sub_addr.str()
              << " (SUB) and bound " << pub_addr.str() << " (PUB)\n";
}


void init_zmq_connection(int pub_port) {
    std::thread([pub_port]() {
        zmq::context_t ctx(1);
        zmq::socket_t pub(ctx, zmq::socket_type::pub);

        std::ostringstream addr;
        addr << "tcp://*:" << pub_port;
        pub.bind(addr.str());

        std::cout << "📡 Monitoring ZMQ publisher bound on " << addr.str() << "\n";

        // Keep thread alive to maintain publisher context
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }).detach();
}

// Initialize a small REP ack server to receive ACK requests from monitor
void init_zmq_ack_server(int ack_port) {
    std::thread([ack_port]() {
        try {
            std::ostringstream addr;
            addr << "tcp://*:" << ack_port;
            ack_rep.bind(addr.str());
            std::cout << "🔁 ACK server bound on " << addr.str() << std::endl;

            while (true) {
                zmq::message_t req;
                if (!ack_rep.recv(req, zmq::recv_flags::none)) continue;
                std::string data(static_cast<char*>(req.data()), req.size());

                // If monitor asks for pending confirmations, reply with one if available
                std::string reply = "";
                {
                    std::lock_guard<std::mutex> lock(ack_mtx);
                    if (!ack_queue.empty()) {
                        reply = ack_queue.front();
                        ack_queue.pop();
                    }
                }

                zmq::message_t res(reply.begin(), reply.end());
                ack_rep.send(res, zmq::send_flags::none);
            }
        } catch (const zmq::error_t& e) {
            std::cerr << "ACK server error: " << e.what() << std::endl;
        }
    }).detach();
}

// === Listen for incoming trade signals from monitoring ===
void start_zmq_listener() {
    std::thread([]() {
        while (true) {
            zmq::message_t msg;
            auto r = exec_sub.recv(msg, zmq::recv_flags::none);
            if (!r || !*r) continue;
            std::string data(static_cast<char*>(msg.data()), msg.size());

            // Log raw incoming ZMQ payload for debugging
            std::cout << "📩 Raw ZMQ signal received: " << data << std::endl;

            // Parse safely and validate fields
            json j;
            try {
                j = json::parse(data);
            } catch (const std::exception& e) {
                std::cerr << "❌ Failed to parse ZMQ message as JSON: " << e.what() << " | payload=" << data << std::endl;
                continue;
            }

            if (!j.contains("symbol") || !j.contains("side") || !j.contains("quantity")) {
                std::cerr << "❌ ZMQ message missing required fields: " << data << std::endl;
                continue;
            }

            std::string symbol = j.value("symbol", "");
            std::string side = j.value("side", "");
            double qty = 0.0;
            double entry_price = 0.0;
            try { qty = j.value("quantity", 0.0); } catch (...) { qty = 0.0; }
            try { entry_price = j.value("entry_price", 0.0); } catch (...) { entry_price = 0.0; }

            std::cout << "📩 Received signal: " << symbol << " " << side
                      << " qty=" << qty << " entry_price=" << entry_price << std::endl;
            {
                std::ostringstream _oss;
                _oss << "📩 Received signal: " << symbol << " " << side << " qty=" << qty << " entry_price=" << entry_price;
                log_to_dashboard(_oss.str());
            }

            // async order execution with diagnostic log
            std::thread([symbol, side, qty, entry_price]() {
                std::cout << "🔧 dispatching send_market_order for " << symbol << " (side=" << side << ")\n";
            }).detach();
        }
    }).detach();
}

// === Send confirmation back to monitoring ===
void send_confirmation(const std::string& symbol, const std::string& status) {
    json msg = {{"symbol", symbol}, {"status", status}};
    std::string data = msg.dump();

    // Publish immediately (best-effort)
    try {
        zmq::message_t zmsg(data.begin(), data.end());
        exec_pub.send(zmsg, zmq::send_flags::dontwait);
        std::cout << "📤 Published confirmation (best-effort): " << data << std::endl;
    } catch (...) {
        // ignore
    }

    // Also push into ack queue so monitor can poll for guaranteed delivery
    {
        std::lock_guard<std::mutex> lock(ack_mtx);
        ack_queue.push(data);
    }
}


void subscriber() {
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    sub.connect("tcp://127.0.0.1:5558");
    sub.set(zmq::sockopt::subscribe, "");
    std::cout << "🔔 Subscribed.\n";

    zmq::message_t msg;
    auto rr = sub.recv(msg, zmq::recv_flags::none);
    if (!rr || !*rr) return;
    std::string data(static_cast<char*>(msg.data()), msg.size());
    std::cout << "✅ Got: " << data << std::endl;
}

// === Monitor-layer subscriber for confirmations ===
void listen_for_closures(int pub_port) {
    std::thread([pub_port]() {
        zmq::context_t ctx(1);
        zmq::socket_t sub(ctx, zmq::socket_type::sub);

        std::ostringstream addr;
        // Allow EXECUTION host to be provided via env for container deployments
        const char* exec_host_env = std::getenv("EXECUTION_PUB_HOST");
        std::string exec_host = exec_host_env ? exec_host_env : std::string("127.0.0.1");
        addr << "tcp://" << exec_host << ":" << pub_port;
        sub.connect(addr.str());
        sub.set(zmq::sockopt::subscribe, "");

        std::cout << "🔔 Listening for close notifications on " << addr.str() << "\n";

        while (true) {
            zmq::message_t msg;
            if (!sub.recv(msg, zmq::recv_flags::none)) continue;
            std::string data(static_cast<char*>(msg.data()), msg.size());
            json j = json::parse(data, nullptr, false);
            if (!j.is_object()) continue;

            std::string symbol = j.value("symbol", "");
            std::string status = j.value("status", "");
            if (status == "CLOSED" || status == "EXISTING") {
                notify_trade_inactive(symbol, "EXECUTION_CONFIRMED");
                std::cout << "✅ Received CLOSED for " << symbol << std::endl;
            }
        }
    }).detach();
}