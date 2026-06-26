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


# There are 3 delivery guarantee mechanisms:
# 1. At-Least-Once -> Minimun one time -> No data loss but might cause duplicate data
# 2. At-Most-Once -> Maximum one time -> Data can be lost
# 3. Exactly-Once -> No data loss or data duplicates -> Cannot be achieved just by using kafka

# 1. At-Least-Once:
# msg -> read -> process -> commit -> msg -> read -> process -> crash
# Same data is processed again which causes duplicates


# 2. At-Most-Once
# msg -> read -> commit -> process -> msg -> read -> commit -> crash
# Now the message cannot be recovered for processing since it has already been commited so the data is lost


# 3. Exactly-Once
# Msg is only delivered once
# It treats msg -> read -> process -> commit as one single atomic transaction
# So for the transaction to be completed all the parts must be completed
# If any part fails then it is rolled back to the initial state until there is completion of all the parts
# This prevents from data loss as well as duplication

# eg: msg -> read -> process(database insert) -> commit = Single transaction

# If it was not a single transaction then:
# 1. msg -> read -> database insert -> commit -> msg -> read -> database insert -> crash
# -> There is duplication of data in the database since the data during server crash is already inserted in the database
# If it is a single transaction then:
# 2. msg -> read -> database insert -> commit -> msg -> read -> database insert -> crash 
# -> The incomplete changes are rolled back so no data duplication as well as data loss
