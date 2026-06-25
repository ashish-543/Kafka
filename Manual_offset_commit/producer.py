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
    time.sleep(5)

producer.flush()



# Each topic has its own offset that starts from 0 so there is no universal offset
# When a consumer in a consumer group reads message then it has to tell kafka about the offset of that message
# By default, consumer auto commits the offset value but this auto commit feature might cause problems if the data is read and processed
# but not yet commited and if the consumer crashes then the same data is read twice because kafka only sees the recent commit which is the previous
# one so this causes duplicate processing

# Kafka stores this information in __consumer_offsets

# Better practice to use manual commit
# "enable.auto.commit": True in consumer_config


# Case 1:
## Read data -> Process data -> commit message -> kill consumer == consumer crashes after committing message
# msg1 -> read -> process -> commit -> msg2 -> read -> process -> commit -> consumer off
# consumer turned back on
# msg3 -> read -> process -> commit -> .............and so on.

# In this case, since the message is commited and then the consumer crashed so the offset value is set to the latest message processed
# So there is no duplicate processing


# Case 2:
## Read data -> Process data -> kill consumer -> commit == consumer crashes before committing message
# For this case, in the consumer code, add time.sleep(time) before committing to imitate the delay in message commiting.

# msg1 -> read -> process -> commit -> msg2 -> read -> process -> consumer off
# consumer turned back on
# msg2 -> read -> process -> commit -> msg3 -> read -> process -> commit -> .............and so on.

# In this case, there is duplicate processing of msg2.