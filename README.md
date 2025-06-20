# confluent_genai_hackathon
confluent genAi hackathon day
Stock Anomaly Detection is a real-time streaming AI application built on Confluent Cloud that identifies abnormal stock trades and potential fraud patterns using both streaming analytics and machine learning.

The solution processes synthetic stock trade data generated via Datagen Source Connector and delivers anomaly detection through two parallel processing paths: Flink SQL queries and a Python-based AI model. Architecture Overview:
🔸 Source:
Used the Datagen Source Connector to simulate live trading data.

Trades are streamed into a Kafka topic: datagen-topic, containing fields like:

json
Copy
Edit
{
  "side": "SELL",
  "quantity": 3385,
  "symbol": "ZTEST",
  "price": 870,
  "account": "LMN456",
  "userid": "User_7"
}
✅ Anomaly Detection Paths:
1️⃣ Flink SQL-Based Rule Detection
Used Flink SQL workspace in Confluent Cloud to:

Filter trades with quantity or price above threshold.

Extract and transform raw Kafka message data.

Insert suspicious records into anomalies-topic.

Example rule:

sql
Copy
Edit
SELECT * FROM datagen-topic
WHERE CAST(JSON_VALUE(CAST(val AS STRING), '$.quantity') AS INT) > 3000;
✅ Purpose: Fast, rule-driven alerts for obvious anomalies.

2️⃣ Python Client + AI Model Detection
Created a Python Kafka consumer using confluent_kafka.

Consumed data from datagen-topic, parsed JSON, and applied a scikit-learn Isolation Forest model.

Trades flagged as outliers were published into a separate Kafka topic: fraud-alerts-topic.

✅ Purpose: Detect hidden, behavior-based anomalies using unsupervised learning.

🧰 Tech Stack:
Confluent Cloud (Kafka, Connect, Schema Registry, Flink SQL)

Datagen Source Connector (for simulating trades)

Apache Flink SQL (for real-time streaming filters)

Python (for AI model and Kafka integration)

Scikit-learn (IsolationForest for anomaly detection)
