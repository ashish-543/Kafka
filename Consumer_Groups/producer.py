from confluent_kafka import Producer
import uuid
import time
import json
import random


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
        
names = ["alice", "dogen", "charlie", "david", "giancarlo", "anderson", "grace"]
items = ["burger", "pizza", "dumplings", "juice", "hotdog"]

for i in range(1, 50):
    values = {
        "order_id": str(uuid.uuid4()),
        "user_name": random.choice(names),
        "items": random.choice(items),
        "quantity": i
    }
    value = json.dumps(values).encode("utf-8")

    producer.produce(
        topic = "orders",
        key = values["user_name"].encode("utf-8"),
        value = value,
        callback = delivery_report
    )
    time.sleep(2)

producer.flush()



# In out given system, there are three partitions.

# Case 1 : 3 partitions & 3 Consumers in a Consumer Group

# Partition 0 -> assigned to -> Consumer1.py
# Partition 1 -> Consumer2.py
# Partition 2 -> Consumer3.py


# Case 2 : 3 partitions & 4 Consumers in a CG

# Partition 0 -> Consumer1.py
# Partition 1 -> Consumer2.py
# Partition 2 -> Consumer3.py
# Consumer4 -> Idle


# Rebalancing -> Rebalancing the partition assignment when consumer number changes in the consumer group
# Case 3 : 3 partitions & 3 Consumers in a CG but consumer2.py is stopped during the process

## Initially,
# Partition 0 -> Consumer1.py
# Partition 1 -> Consumer2.py
# Partition 2 -> Consumer3.py

## Consumer2.py stopped,

# Partition 0 -> Consumer1.py
# Partition 1 -> Consumer1.py
# Partition 2 -> Consumer3.py

# i.e when a consumer fails during the process its roles i.e partitions are assignedd to other consumers.


# Case 4 : 3 partitions & 4 Consumers in a CG but consumer2.py is stopped during the process

## Initially,
# Partition 0 -> Consumer1.py
# Partition 1 -> Consumer2.py
# Partition 2 -> Consumer3.py
# Consumer4 -> Idle

## consumer2.py stopped
# Partition 0 -> Consumer1.py
# Partition 1 -> Consumer4.py
# Partition 2 -> Consumer3.py


# Conslusion:
# Paritions provide the parallelism feature to the kafka not the consumers
# The partition assignment to the consumers after rebalancing is not fixed i.e any consumer can get multiple partitions
# So partition assignment is deterministic for a set of consumers but is non-deterministic if consumers leave or join the consumer group(rebalancing)
