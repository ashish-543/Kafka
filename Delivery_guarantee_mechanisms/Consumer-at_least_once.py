import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "tracker", 
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(2)
        if msg is None:
            continue
        if msg.error():
            print("Error Occured:", msg.error())
            continue
        value = json.loads(msg.value().decode("utf-8"))
        print("Received value: ", value)
        print(f"""
                Key        : {msg.key().decode("utf-8")}
                Topic      : {msg.topic()}
                Partition  : {msg.partition()}
                Offset     : {msg.offset()}
        """)
        consumer.commit(msg) # Commiting after processing
except KeyboardInterrupt:
    print("Stopping Consumer")
finally:
    consumer.close()