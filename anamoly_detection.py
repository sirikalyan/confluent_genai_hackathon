from sklearn.ensemble import IsolationForest
import json
from confluent_kafka import Consumer, Producer

# Load or simulate some training data (fake normal trades)
# Train model

model = IsolationForest(contamination=0.1)
model.fit([[100, 200], [110, 220], [120, 210], [9500, 8900]])  # quantity, price


# Kafka setup
consumer = Consumer({
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'group.id': 'fraud-detector',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'QK7VSYQ23JI3NBL4',
    'sasl.password': 'slBnS5QRvdriWH87fbui2yRfQGKA7wYp7b8W1BWnR9inUGnKN9cdG5SWZyFNvCol',
})
consumer.subscribe(['datagen-topic'])

producer = Producer({'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'})
print("came")
# Inference loop
while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    data = json.loads(msg.value().decode('utf-8'))

    # Score using AI model
    features = [[data['quantity'], data['price']]]
    is_outlier = model.predict(features)[0] == -1

    if is_outlier:
        print("🚨 FRAUD DETECTED:", data)
        producer.produce('fraud-alerts-topic', json.dumps(data).encode('utf-8'))
