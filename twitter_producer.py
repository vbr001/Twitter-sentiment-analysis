from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime


# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

faker = Faker()
topics = ['twitter-topic1','twitter-topic2', 'twitter-topic3']
# Tweet sentiment templates using Faker dynamically
def generate_tweet():
    sentiment = random.choice(['positive', 'negative', 'neutral'])
    
    if sentiment == 'positive':
        text = random.choice([
            f"Loving the new product from {faker.company()}! Great job team! #innovation",
            f"Had an amazing lunch at {faker.company()} — highly recommend! #foodie #happy",
            f"Shoutout to {faker.name()} for their awesome work today!",
            f"Feeling blessed visiting {faker.city()} #travel #grateful",
            f"{faker.name()} just dropped a great blog on {faker.bs()}!"
        ])
    elif sentiment == 'negative':
        text = random.choice([
            f"Really disappointed with service at {faker.company()} today. #fail",
            f"Ugh... Monday meetings with {faker.name()} again",
            f"Why does {faker.company()} always mess things up? #frustrated",
            f"Terrible experience using {faker.catch_phrase()}",
            f"{faker.name()} forgot the deadline. Not again."
        ])
    else:  # neutral
        text = random.choice([
            f"Just arrived at {faker.city()} for a conference. #event",
            f"Reading up on {faker.bs()} today.",
            f"{faker.name()} mentioned an interesting idea at lunch. #idea",
            f"Anyone been to {faker.company()} HQ? Curious how it looks.",
            f"Working on a project related to {faker.catch_phrase()}."
        ])
    
    tweet = {
        "text": text,
        "timestamp": datetime.utcnow().isoformat()
    }
    return tweet

# Send tweets continuously
while True:
    tweet = generate_tweet()
    print(f"Sending tweet: {tweet['text']}")
    for topic in topics:
        producer.send(topic,tweet)
        print(f"Sent to {topic}: {tweet}")
    time.sleep(random.uniform(1, 3))  # simulate human-like delay
