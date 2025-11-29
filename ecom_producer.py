import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'ecom_clickstream_raw'

# Sample data
USER_IDS = [f"user_{i:04d}" for i in range(1, 501)]
PRODUCT_IDS = [f"prod_{i:04d}" for i in range(1, 101)]
EVENT_TYPES = ['view', 'add_to_cart', 'purchase']
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']

HIGH_INTEREST_PRODUCT = "prod_0042"

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        print(f"Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaError as e:
        print(f"Failed to create Kafka producer: {e}")
        raise

def generate_event(event_number):
    if random.random() < 0.2:
        product_id = HIGH_INTEREST_PRODUCT
        event_type = random.choices(EVENT_TYPES, weights=[0.95, 0.04, 0.01])[0]
    else:
        product_id = random.choice(PRODUCT_IDS)
        event_type = random.choices(EVENT_TYPES, weights=[0.70, 0.20, 0.10])[0]

    user_id = random.choice(USER_IDS)

    event = {
        'event_id': f"evt_{event_number:08d}",
        'user_id': user_id,
        'product_id': product_id,
        'event_type': event_type,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'session_id': f"session_{random.randint(1000, 9999)}",
        'product_category': random.choice(PRODUCT_CATEGORIES),
        'device_type': random.choice(['mobile', 'desktop', 'tablet']),
        'page_url': f"/product/{product_id}",
        'price': round(random.uniform(10.0, 1000.0), 2)
    }

    return event

def send_event(producer, event):
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=event['user_id'],
            value=event
        )
        record_metadata = future.get(timeout=10)

        print(f"Sent: {event['event_type']:15s} | "
              f"User: {event['user_id']:10s} | "
              f"Product: {event['product_id']:10s} | "
              f"Partition: {record_metadata.partition} | "
              f"Offset: {record_metadata.offset}")

    except KafkaError as e:
        print(f"Failed to send event: {e}")

def produce_stream(duration_seconds=60, events_per_second=5):
    producer = create_producer()
    event_number = 1
    start_time = time.time()

    print("\nStarting event production...")
    print("=" * 60)

    try:
        while time.time() - start_time < duration_seconds:
            for _ in range(events_per_second):
                event = generate_event(event_number)
                send_event(producer, event)
                event_number += 1

            time.sleep(1)

    except KeyboardInterrupt:
        print("Producer interrupted.")

    finally:
        producer.flush()
        producer.close()
        print(f"\nProducer closed. Total events sent: {event_number - 1}")

if __name__ == "__main__":
    produce_stream(duration_seconds=60, events_per_second=5)
