import uuid # Universally Unique Identifiers (UUIDs), generates random unique ids 
import json

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

# The location of the kafka is specified in this config.
# In the kafka_compose.yaml file, the specified location is PLAINTEXT://localhost:9092 in KAFKA_ADVERTISED_LISTENERS
# Since producer is a client so it uses this address to communicate with kafka.

producer = Producer(producer_config)


# Creating callback function for producer delivery report
def delivery_status(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print("Message successfully delivered")


order = {
    "order_id": str(uuid.uuid4()),
    "user_name": "sam",
    "product": "burger",
    "quantity": 5
}

# Kafka only accepts raw bytes. So before sending the data, first it has to be serialized into raw bytes using utf-8
# Producer -> serialize to raw bytes
# Consumer -> deserialize to json 

# So for this, the json is first converted to string using .dumps() and then into raw bytes using utf-8
value = json.dumps(order).encode("utf-8")


# Creating producer i.e creating event
producer.produce(
    topic = "orders",
    value = value,
    callback = delivery_status
)

producer.flush()
# The producer doesn't immediately send the message to the broker. It waits for a certain number of messages to be received.
# Then only it sends the messages to the broker so if the producer crashes while collecting the messages then the data will be lost.
# So to prevent this problem, flush() is used which enales the producer to send the messages immediately instead of waiting for other messages