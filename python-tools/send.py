import zmq

def main():
    # Create a ZeroMQ context
    ctx = zmq.Context()

    # Create a PUB (publisher) socket
    socket = ctx.socket(zmq.PUB)

    # === Choose your endpoint ===
    # For same-machine TCP:
    endpoint = "tcp://localhost:5555"
    # For same-machine IPC (faster, if your bot uses ipc://):
    # endpoint = "ipc:///tmp/virtuum_signals.ipc"

    socket.bind(endpoint)
    print(f"✅ Publisher bound to {endpoint}")
    print("Type a signal like 'AIAUSDT LONG' or 'BTCUSDT SHORT' (type 'exit' to quit)\n")

    while True:
        msg = input("Signal → ")
        if msg.lower() == "exit":
            break
        if not msg.strip():
            continue

        socket.send_string(msg)
        print(f"📤 Sent: {msg}")

    socket.close()
    ctx.term()
    print("👋 Exiting publisher.")

if __name__ == "__main__":
    main()