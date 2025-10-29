#pragma once

#include <zmq.hpp>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>

class ZMQComm {
public:
    // Constructor
    ZMQComm() : context(1) {}

    // -------- Publisher (sender) --------
    void startPublisher(const std::string& endpoint, const std::string& baseMessage, int intervalMs = 1000) {
        zmq::socket_t publisher(context, zmq::socket_type::pub);
        publisher.bind(endpoint);
        std::cout << "✅ Publisher bound to " << endpoint << std::endl;

        int counter = 0;
        while (true) {
            std::string payload = baseMessage + " " + std::to_string(counter++);
            zmq::message_t msg(payload.begin(), payload.end());
            publisher.send(msg, zmq::send_flags::none);
            std::cout << "Sent: " << payload << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        }
    }

    // -------- Subscriber (receiver) --------
    void startSubscriber(const std::string& endpoint) {
        zmq::socket_t subscriber(context, zmq::socket_type::sub);
        subscriber.connect(endpoint);
        subscriber.set(zmq::sockopt::subscribe, "");
        std::cout << "✅ Subscriber connected to " << endpoint << std::endl;

        while (true) {
            zmq::message_t msg;
            subscriber.recv(msg, zmq::recv_flags::none);
            std::string data(static_cast<char*>(msg.data()), msg.size());
            std::cout << "Received: " << data << std::endl;
        }
    }

private:
    zmq::context_t context;
};