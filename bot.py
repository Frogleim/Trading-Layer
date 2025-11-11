import requests
import time
import os
import pandas as pd
# === CONFIG ===
BOT_TOKEN = "7255094958:AAGWZA3Ck_LG9I2IQlj4TH2HCY6fRpX_81o"
BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
CSV_PATH = "trades_log.csv"   # Path to your CSV log file

# === HELPERS ===
def get_updates(offset=None):
    """Fetch updates (messages) from Telegram."""
    params = {"timeout": 60, "offset": offset}
    resp = requests.get(f"{BASE_URL}/getUpdates", params=params)
    if resp.status_code == 200:
        return resp.json()
    return {}

def send_message(chat_id, text):
    """Send a text message to user."""
    data = {"chat_id": chat_id, "text": text}
    requests.post(f"{BASE_URL}/sendMessage", data=data)

def send_csv(chat_id):
    """Send the CSV file to user."""
    if not os.path.exists(CSV_PATH):
        send_message(chat_id, "❌ CSV file not found.")
        return
    df = pd.read_csv(CSV_PATH, sep=None, engine="python")
    df.to_csv(CSV_PATH, index=False)
    with open(CSV_PATH, "rb") as f:
        files = {"document": f}
        data = {"chat_id": chat_id, "caption": "📊 Your latest trade log"}
        resp = requests.post(f"{BASE_URL}/sendDocument", data=data, files=files)

    if resp.status_code == 200:
        print("✅ CSV sent successfully.")
    else:
        print("❌ Failed to send CSV:", resp.text)
        send_message(chat_id, "⚠️ Error sending CSV file.")


# === MAIN LOOP ===
def main():
    print("🤖 Bot started. Waiting for /csv command...")
    last_update_id = None

    while True:
        updates = get_updates(last_update_id)
        if "result" in updates:
            for update in updates["result"]:
                last_update_id = update["update_id"] + 1
                message = update.get("message", {})
                chat_id = message.get("chat", {}).get("id")
                text = message.get("text", "")

                if not text:
                    continue

                print(f"📩 Received: {text} from chat {chat_id}")

                if text.lower() == "/csv":
                    send_csv(chat_id)
                elif text.lower() == "/start":
                    send_message(chat_id, "👋 Send /csv to get your trade log CSV file.")
                else:
                    send_message(chat_id, "❓ Unknown command. Try /csv")

        time.sleep(2)


if __name__ == "__main__":
    main()