import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "tracker", # Defining consumer group id
    "auto.offset.reset": "earliest"  # If the offset value is lost due to server crashing then the message is read from the earliest offset.
}

consumer = Consumer(consumer_config)

# Subscribing to a topic
consumer.subscribe(["orders"]) # A consumer can subscribe to multiple topics inside the topic list.

print("Consumer is running and subscribed to orders topic")


try:
    while True:
        msg = consumer.poll(5)
        if msg is None:
            continue
        if msg.error():
            print("Error Occured:", msg.error())
            continue

        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print("Received value: ", order)
except KeyboardInterrupt:
    print("Stopping Consumer")
finally:
    consumer.close()