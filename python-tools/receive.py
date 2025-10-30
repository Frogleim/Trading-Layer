import zmq

def listen_for_confirmations():
    # 1️⃣ Create context and subscriber socket
    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    # 2️⃣ Connect to publisher (use localhost if running on same machine)
    socket.connect("tcp://localhost:5556")

    # 3️⃣ Subscribe to all topics (empty string = no filter)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")

    print("📡 Listening for trade confirmations on tcp://localhost:5556 ...")

    while True:
        try:
            # 4️⃣ Receive and decode message
            message = socket.recv_string()
            print(f"✅ Received confirmation: {message}")
        except KeyboardInterrupt:
            print("\n🛑 Stopped listening.")
            break
        except Exception as e:
            print(f"❌ Error receiving message: {e}")

    socket.close()
    context.term()

if __name__ == "__main__":
    listen_for_confirmations()