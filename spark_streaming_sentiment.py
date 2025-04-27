from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, udf, col, to_json, struct
from pyspark.sql.types import FloatType, StringType
from textblob import TextBlob

# ----------------------- Sentiment Functions ----------------------- #
def get_polarity(text):
    return TextBlob(text).sentiment.polarity

def get_subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

def get_sentiment_label(score):
    if score > 0:
        return "Positive"
    elif score < 0:
        return "Negative"
    else:
        return "Neutral"

# ----------------------- UDFs ----------------------- #
polarity_udf = udf(get_polarity, FloatType())
subjectivity_udf = udf(get_subjectivity, FloatType())
sentiment_label_udf = udf(get_sentiment_label, StringType())

# ----------------------- Spark Session ----------------------- #
spark = SparkSession.builder \
    .appName("KafkaTweetSentimentToMySQL") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ----------------------- Kafka Read (raw_tweets) ----------------------- #
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter-topic1,twitter-topic2,twitter-topic3") \
    .load() \
    .selectExpr("CAST(value AS STRING) as tweet")

# ✅ Save raw tweets to MySQL table: raw_tweets
def save_raw_to_mysql(df, _):
    try:
        df.select("tweet").write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/tweetdb") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "raw_tweets") \
            .option("user", "sparkuser") \
            .option("password", "Spark@123") \
            .mode("append") \
            .save()
        print("✅ Saved raw tweets to MySQL")
    except Exception as e:
        print(f"❌ Error saving raw tweets: {e}")

raw_mysql_query = lines.writeStream \
    .outputMode("append") \
    .foreachBatch(save_raw_to_mysql) \
    .start()

# ----------------------- Stage 1: Cleaning ----------------------- #
tweets = lines.select(explode(split(col("tweet"), "t_end")).alias("tweet"))
cleaned = tweets.withColumn("cleaned_text", regexp_replace("tweet", r"http\S+|@\S+|#|\n", ""))

# ✅ Write to filtered_tweets Kafka topic
filtered_kafka_query = cleaned.selectExpr("cleaned_text as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "filtered_tweets") \
    .option("checkpointLocation", "/tmp/checkpoint_filtered") \
    .outputMode("append") \
    .start()

# ✅ Save only cleaned tweets to MySQL table: filtered_tweets
def save_filtered_to_mysql(df, _):
    try:
        df.select("cleaned_text").write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/tweetdb") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "filtered_tweets") \
            .option("user", "sparkuser") \
            .option("password", "Spark@123") \
            .mode("append") \
            .save()
        print("✅ Saved filtered tweets to MySQL")
    except Exception as e:
        print(f"❌ Error saving filtered tweets: {e}")

filtered_mysql_query = cleaned.writeStream \
    .outputMode("append") \
    .foreachBatch(save_filtered_to_mysql) \
    .start()

# ----------------------- Stage 2: Sentiment Analysis ----------------------- #
scored = cleaned.withColumn("polarity", polarity_udf(col("cleaned_text"))) \
                .withColumn("subjectivity", subjectivity_udf(col("cleaned_text"))) \
                .withColumn("sentiment", sentiment_label_udf(col("polarity")))

# ✅ Write to scored_tweets Kafka topic
scored_kafka_query = scored.selectExpr("to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "scored_tweets") \
    .option("checkpointLocation", "/tmp/checkpoint_scored") \
    .outputMode("append") \
    .start()

# ✅ Save cleaned + sentiment info to MySQL table: scored_tweets
def save_scored_to_mysql(df, _):
    try:
        df.select("cleaned_text", "polarity", "subjectivity", "sentiment").write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/tweetdb") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "scored_tweets") \
            .option("user", "sparkuser") \
            .option("password", "Spark@123") \
            .mode("append") \
            .save()
        print("✅ Saved scored tweets to MySQL")
    except Exception as e:
        print(f"❌ Error saving scored tweets: {e}")

scored_mysql_query = scored.writeStream \
    .outputMode("append") \
    .foreachBatch(save_scored_to_mysql) \
    .start()

# ----------------------- Await All Queries ----------------------- #
raw_mysql_query.awaitTermination()
filtered_kafka_query.awaitTermination()
filtered_mysql_query.awaitTermination()
scored_kafka_query.awaitTermination()
scored_mysql_query.awaitTermination()
