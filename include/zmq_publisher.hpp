#pragma once
#include <string>
#include <zmq.hpp>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

extern zmq::context_t exec_ctx;
extern zmq::socket_t exec_sub;
extern zmq::socket_t exec_pub;

// --- Initialize subscriber & publisher sockets with configurable ports ---
void init_zmq_execution(int sub_port = 5557, int pub_port = 5558);

// --- Background thread to handle incoming signals ---
void start_zmq_listener();

// --- Publish confirmation back to monitoring layer ---
void send_confirmation(const std::string& symbol, const std::string& status);

// --- Subscribe to confirmations from execution layer (CLOSED events) ---
void listen_for_closures(int pub_port = 5558);


void init_zmq_connection(int pub_port);

// --- Reliable confirmation (REQ/REP)
void init_zmq_ack_server(int ack_port = 5560);