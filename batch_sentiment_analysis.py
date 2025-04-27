'''from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TweetBatchSentiment") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from MySQL database
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/tweetdb") \
    .option("dbtable", "tweet_sentiments") \
    .option("user", "sparkuser") \
    .option("password", "Spark@123") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Perform sentiment aggregation
agg_df = df.groupBy("sentiment") \
    .agg(
        count("*").alias("tweet_count"),
        avg("polarity").alias("average_polarity")
    )

# Show results in terminal
print("\n Sentiment Aggregation Results:")
agg_df.show(truncate=False)

# Optional: Save to a CSV file (comment if not needed)
# agg_df.coalesce(1).write.csv("/home/osboxes/DBT_PROJECT_cmplt/output/sentiment_summary", header=True, mode="overwrite")

# Stop Spark session
spark.stop()'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
import time

start_time = time.time()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TweetBatchSentiment") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from MySQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/tweetdb") \
    .option("dbtable", "scored_tweets") \
    .option("user", "sparkuser") \
    .option("password", "Spark@123") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Aggregate
agg_df = df.groupBy("sentiment").agg(
    count("*").alias("tweet_count"),
    avg("polarity").alias("average_polarity")
)

print("\n=== BATCH MODE RESULTS ===")
agg_df.show(truncate=False)

end_time = time.time()
print(f"\n⏱️ Batch Mode Execution Time: {end_time - start_time:.2f} seconds")

spark.stop()


