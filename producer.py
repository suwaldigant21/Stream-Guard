import json
import os
import time

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

from streamguard_serializers import JSONSerializer

load_dotenv()

# Configuration (from .env; only non-secrets get fallbacks).
API_URL = os.getenv("STREAMGUARD_API_URL", "http://127.0.0.1:8000/v1/transactions")
API_KEY = os.getenv("STREAMGUARD_API_KEY")
if not API_KEY:
    print("WARNING: STREAMGUARD_API_KEY is not set - API calls will be rejected.")
REDPANDA_BROKER = os.getenv("STREAMGUARD_REDPANDA_BROKER", "localhost:19092")
TOPIC_NAME = os.getenv("STREAMGUARD_TOPIC", "transactions-raw")
BATCH_SIZE = int(os.getenv("STREAMGUARD_BATCH_SIZE", "20"))
SLEEP_INTERVAL = float(os.getenv("STREAMGUARD_SLEEP_INTERVAL", "0.5"))
# Watermark file: the last streamed API offset. On restart the producer resumes
# from here instead of re-streaming from 0, so every transaction reaches the
# topic exactly once (the consumer checkpoint does the same for Bronze).
# Delete this file to intentionally restart from offset 0.
STATE_FILE = os.getenv("STREAMGUARD_PRODUCER_STATE", "data/producer_state.json")

# Initialize Kafka/Redpanda Producer
producer = KafkaProducer(
    bootstrap_servers=[REDPANDA_BROKER],
    value_serializer=JSONSerializer(),
    acks="all",
)


def load_state() -> int:
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return int(json.load(f).get("offset", 0))
    except (OSError, ValueError):
        return 0


def save_state(offset: int) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"offset": offset}, f)
    except OSError as e:
        print(f"WARNING: could not persist producer offset: {e}")


def run_producer(batch_size=BATCH_SIZE, sleep_interval=SLEEP_INTERVAL):
    offset = load_state()
    headers = {"X-API-Key": API_KEY}

    if offset:
        print(f"Resuming from watermark offset {offset} (delete '{STATE_FILE}' to restart from 0).")
    else:
        print("No watermark found - starting from offset 0.")

    print(f"Starting producer stream to Redpanda topic '{TOPIC_NAME}'...")

    try:
        while True:
            try:
                # Fetch transaction chunk from FastAPI
                response = requests.get(
                    API_URL,
                    headers=headers,
                    params={"offset": offset, "limit": batch_size},
                    timeout=10,
                )

                if response.status_code != 200:
                    print(f"Error fetching data: {response.status_code} - {response.text}")
                    time.sleep(2)
                    continue

                payload = response.json()
                records = payload.get("data", [])

                if not records:
                    print("Reached end of dataset feed.")
                    break

                # Send each transaction to Redpanda
                for txn in records:
                    producer.send(TOPIC_NAME, value=txn)

                producer.flush()
                print(f"Streamed {len(records)} events (Offset {offset} -> {offset + len(records)})")

                offset += len(records)
                save_state(offset)
                time.sleep(sleep_interval)  # Controls streaming pace

            except (requests.RequestException, KafkaError, ValueError) as e:
                print(f"Producer error: {e}")
                time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
