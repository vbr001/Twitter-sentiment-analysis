import tweepy
import os

# Set your bearer token (Keep it secret!)
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAKvU0QEAAAAAwb%2BvjsLjLg%2B65NpGzV%2FkmeaV55E%3DDhOtdTZJnz2tGdy9MufkE6fRcpMryCV06HiVXmGZ6Yr0RVUBd3"

# Initialize Twitter client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

def search_tweets(query, max_results=10):
    """
    Fetches recent tweets for a given query using Twitter API v2.

    Args:
        query (str): The search query (e.g., "AI", "openai langchain")
        max_results (int): Max number of tweets to return (default: 10)

    Returns:
        List of tweet texts
    """
    response = client.search_recent_tweets(query=query, max_results=max_results, tweet_fields=['lang'])
    tweets = []

    if response.data:
        for tweet in response.data:
            if tweet.lang == 'en':
                tweets.append(tweet.text)
    return tweets

if _name_ == "_main_":
    query = input("Enter keyword to search tweets: ")
    tweets = search_tweets(query, max_results=20)
    print("\nFetched Tweets:\n")
    for idx, tweet in enumerate(tweets, 1):
        print(f"{idx}. {tweet}\n")
