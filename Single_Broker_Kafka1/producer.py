import json
import uuid
import random
import time
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config) 

def delivery_report(err, msg):
    if err:
        print(f"Error Occured: {err}")
    else:
        print(f"""
        Topic       : {msg.topic()}
        Partition   : {msg.partition()}
        Offset      : {msg.offset()}
        Timestamp   : {msg.timestamp()}
""")

names = ["will", "bill", "sam", "levi"]
items = ["burger", "pizza", "dumplings", "juice", "hotdog"]

for i in range(1, 11):
    values = {
        "order_id": str(uuid.uuid4()),
        "user_name": random.choice(names),
        "item": random.choice(items),
        "quantity": i
    }

    value = json.dumps(values).encode("utf-8")

    producer.produce(
        topic = "orders",
        value = value,
        callback = delivery_report
    )
    time.sleep(2)

producer.flush()
