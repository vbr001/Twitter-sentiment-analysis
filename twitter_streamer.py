from kafka import KafkaConsumer
import threading
import json

# Consumer logic for a single topic
def consume(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='twitter-group',  # Same group so Kafka will load-balance
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"Started consumer for topic: {topic_name}")
    for message in consumer:
        tweet = message.value
        print(f"Received from {topic_name}: {tweet}")

# Topics list
topics = ['twitter-topic1', 'twitter-topic2', 'twitter-topic3']

# Create and start a thread for each topic
threads = []
for topic in topics:
    t = threading.Thread(target=consume, args=(topic,))
    t.start()
    threads.append(t)

# Join all threads
for t in threads:
    t.join()
