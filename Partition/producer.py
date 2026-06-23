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

for i in range(1, 11):
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


# The data having same keys are kept in same partition in the order in which they are sent


# First remove the previous topics and then create new topic and specify the paritions and replication_factor inside the topic using CLI:
# -> docker exec -it kafka kafka-topics `
# -> --create `
# -> --topic orders `
# -> --partitions 3 `
# -> --replication-factor 1 `
# -> --bootstrap-server localhost:9092

# -> This creates topic orders with 3 paritions and replication_factor is 1 for the orders topic
# This replication factor is for topic whereas the replication_factor in the config file is for __consumer_offsets topic
# The __consumer_offsets topic is the default topic that keeps the metadata of consumer group i.e Stores consumer group offsets

# Now to see the topics in the kafka:
# -> docker exec -it kafka kafka-topic `
# -> --describe `
# -> --topic orders `
# -> --bootstrap-server localhost:9092
# Output:
# Topic: orders   TopicId: EU_G-LuDT4etM1hMJ3w26g PartitionCount: 3       ReplicationFactor: 1    Configs: 
#         Topic: orders   Partition: 0    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
#         Topic: orders   Partition: 1    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
#         Topic: orders   Partition: 2    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr: 

# The partition is calculated using: hash(key)%partition_number
# -> This doesnot guarantee equal distribution of data across the partition
# -> It also doesnot guarantee that every partition gets data.


# If the keys are not mentioned then the partition selection will be random
# Kafka first receives a batch of messages and then decides which partition to use on the go randomly and it sticks to that partition for that current batch
# This is called sticky method where it sticks to a partition for a given batch.
# After the current batch is finished then the process is repeated for the next batch
# So in this method, there is no feature of data being in the same partition based on keys
# It guarantees that every partition gets data