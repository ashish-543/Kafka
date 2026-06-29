# 🚀 Apache Kafka for Data Engineering

A hands-on repository covering **Apache Kafka fundamentals** through practical experiments using **Python**, **Docker**, and **Confluent Kafka**. This repository is designed to build a strong foundation before integrating Kafka with **PySpark**, **Databricks**, and modern data engineering pipelines.

---

## 📚 Topics Covered

* ✅ Single Broker Kafka (KRaft Mode)
* ✅ Producers & Consumers
* ✅ Message Serialization (JSON)
* ✅ Producer Keys & Partitioning
* ✅ Topics & Partitions
* ✅ Consumer Groups
* ✅ Consumer Rebalancing
* ✅ Manual Offset Commit
* ✅ Delivery Guarantee Mechanisms
* ✅ Multi-Broker Kafka Cluster
* ✅ Leader & Follower Replicas
* ✅ Controller Leader vs Partition Leaders

---

## 📂 Repository Structure

```text
Kafka/
│
├── 1. Single_Broker_Kafka/
│   ├── producer.py
│   ├── consumer.py
│   └── kafka_compose.yaml
│
├── 2. Single_Broker_Kafka1/
│   ├── producer.py
│   ├── consumer.py
│   └── kafka_compose.yaml
│
├── 3. Consumer_Groups/
│   ├── producer.py
│   ├── consumer1.py
│   ├── consumer2.py
│   ├── consumer3.py
│   ├── consumer4.py
│   └── kafka_compose.yaml
│
├── 4. Delivery_Guarantee_Mechanisms/
│
├── 5. Manual_Offset_Commit/
│
├── 6. Partitions/
│
└── 7. Multiple_Broker/
```

---

# 📖 Folder Description

## 1️⃣ Single_Broker_Kafka

Implementation of a **single broker Kafka cluster** in **KRaft mode** with a basic producer and consumer.

### Concepts Covered

* Kafka Producer
* Kafka Consumer
* JSON Serialization & Deserialization
* Topics
* Offsets
* Delivery Reports

---

## 2️⃣ Single_Broker_Kafka1

Extends the previous implementation by introducing **Message Keys**.

### Concepts Covered

* Producer Keys
* Hash-based Partitioning
* Deterministic Message Routing
* Key-based Ordering

---

## 3️⃣ Consumer_Groups

Experiments demonstrating Kafka's **Consumer Group** architecture.

### Experiments Performed

* Multiple Consumers
* Consumer Group Coordination
* Partition Assignment
* Rebalancing
* Idle Consumers
* Consumer Failure Recovery

---

## 4️⃣ Delivery_Guarantee_Mechanisms

Implementation of Kafka's delivery semantics through practical experiments.

### Concepts Covered

* At-Most-Once Delivery
* At-Least-Once Delivery
* Exactly-Once (Theory)
* Producer Acknowledgements
* Duplicate Processing

---

## 5️⃣ Manual_Offset_Commit

Understanding how Kafka manages offsets manually.

### Concepts Covered

* Auto Commit vs Manual Commit
* Offset Management
* Crash Recovery
* Duplicate Reads
* Reliable Processing

---

## 6️⃣ Partitions

Experiments focused on Kafka partitions and message distribution.

### Concepts Covered

* Topic Partitions
* Key-based Routing
* Sticky Partitioner
* Ordering Guarantees
* Parallel Processing

---

## 7️⃣ Multiple_Broker

Implementation of a **3-Broker Kafka Cluster** using Docker.

### Concepts Covered

* Multi-Broker Cluster
* Controller Quorum (KRaft)
* Bootstrap Servers
* Replication
* Leader & Follower Replicas
* Controller Leader
* Partition Leaders
* Topic Replication

---

# 🛠️ Tech Stack

* Python
* Apache Kafka
* Confluent Kafka Python Client
* Docker
* Docker Compose
* JSON

---

# 🎯 Learning Outcome

After completing this repository, you will understand:

* How Kafka Producers and Consumers communicate
* Topic creation and partitioning strategies
* Consumer Groups and rebalancing
* Offset management
* Delivery guarantee mechanisms
* Multi-broker Kafka architecture
* Replication fundamentals
* Leader vs Follower replicas
* Controller Leader vs Partition Leaders

This repository serves as a strong foundation before moving to advanced topics such as **Kafka + Spark Structured Streaming**, **Databricks**, **Delta Lake**, and production-grade real-time data pipelines.

---

# 👨‍💻 Author

**Ashish Pandeya**

- Data Engineering & AI Enthusiast
- Interested in Data Engineering, Machine Learning, Deep Learning, and Cloud Technologies

---

## 🤝 Contributions

Suggestions, improvements, and feedback are always welcome. Feel free to fork the repository, open an issue, or submit a pull request.

---

# ⭐ If you found this repository helpful, consider giving it a star!
