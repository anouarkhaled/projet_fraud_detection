import json
import time
import random
import os
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

# 🔹 Load environment variables
load_dotenv()

# ⚙️ Configuration depuis le fichier .env
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactions")
SLEEP_TIME = float(os.getenv("PRODUCER_SLEEP", 0.5))

# 🔹 Initialize Faker and Kafka Producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10
)

def make_transaction():
    """Generate a fake transaction."""
    return {
        "transaction_id": fake.uuid4(),
        "client_id": str(random.randint(1000, 9999)),
        "amount": round(random.uniform(1, 5000), 2),
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": fake.city(),
        "device": random.choice(["mobile", "web", "pos"])
    }

if __name__ == "__main__":
    print(f"🚀 Starting producer on topic '{TOPIC_NAME}' at broker '{KAFKA_BROKER}'")
    try:
        while True:
            tx = make_transaction()
            producer.send(TOPIC_NAME, tx)
            print("✅ Sent:", tx)
            time.sleep(SLEEP_TIME)
            producer.flush()
    except KeyboardInterrupt:
        print("🛑 Stopping producer...")
        producer.close()
