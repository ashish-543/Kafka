from confluent_kafka import Producer
import uuid
import time
import json
import random


producer_config = {
    "bootstrap.servers": "localhost:9092, localhost:9094, localhost:9096",
    "acks": "all",
    "retries": 10,
    "enable.idempotence": True
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


# Problem: Create 3 paritions with replication count 3 i.e 3 replicas of the data, one original and two copies
# We have 3 brokers so
# partition0 -> leader -> broker1
# partition1 -> leader -> broker2
# partition2 -> leader -> broker3

# The followers i.e replicas are selected in round robin i.e the brokers that come after the leader
# partition0 -> leader 1 -> replicas -> 2&3
# partition1 -> leader 2 -> replicas -> 3&1
# partition2 -> leader 3 -> replicas -> 1&2

# code:
# docker exec -it broker1 kafka-topics `
# --create `
# --topic orders `
# --partitions 3 `
# --replication-count 3 `
# --bootstrap-server localhost:9092

# Question: We have three brokers but we have only created topic in one broker?
# -> It is because Kafka only needs one entry point into the cluster
# -> Once kafka reaches the cluster, the request is then sent to leader broker(controller leader) which could be any of the three brokers
# So the flow is :
# Docker ClI -> Broker 1 -> Controller Leader(Cluster Leader) -> Topic Created
# The cluster leader is diferent than the partition leader.
# The cluster leader is responsible for managing the cluster metadata similar to the zookeeper
# There is only controller leader in a cluster
# The cluster leader and partition leader can also be same broker

# To check which broker is the cluster leader:
# docker exec -it broker1 kafka-metadata-quorum `
# --bootstrap-server localhost:9092 `               -> Here 9092 is the container port so using 9094 and 9096 won't work whereas you can use 9092 for all three brokers
#  describe --status

# ClusterId:              MkU3OEVBNTcwNTJENDM2Qk
# LeaderId:               2 -> Leader id is 2 so -> Broker 2 is the cluster leader
# LeaderEpoch:            2
# HighWatermark:          5015
# MaxFollowerLag:         0
# MaxFollowerLagTimeMs:   214
# CurrentVoters:          [1,2,3]


# To check the leader and followers in each partitions
# docker exec -it broker1 kafka-topics `            
# --describe `
# --topic orders `
# --bootstrap-server localhost:9092

# Topic: orders   TopicId: wW3KXepeQBWAzAW-_ZoF2Q PartitionCount: 3       ReplicationFactor: 3    Configs: 
#        Topic: orders   Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:    LastKnownElr: 
#        Topic: orders   Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:    LastKnownElr: 
#        Topic: orders   Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr:    LastKnownElr: 